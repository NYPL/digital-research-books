import json
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Literal, Optional, Tuple, Union
from typing_extensions import TypedDict


import newrelic.agent
import numpy as np
import pandas as pd
from agents import (
    Agent,
    ModelSettings,
    OpenAIChatCompletionsModel,
    RunConfig,
    RunContextWrapper,
    RunErrorHandlerInput,
    RunErrorHandlerResult,
    RunHooks,
    Runner,
    RunResult,
    function_tool,
)
from agents.extensions.memory import SQLAlchemySession
from agents.items import ModelResponse
from agents.models.chatcmpl_converter import Converter
from agents.run_config import DEFAULT_MAX_TURNS
from agents.tool_context import ToolContext
from jinja2 import Template
from openai import AsyncOpenAI
from openai.types.shared import Reasoning
from sqlalchemy import text


# api code
from ..utils import remove_markdown_comments
from ..db import (
    get_frbr_data_by_barcode,
    get_frbr_data_by_edition,
    get_readonly_session,
    get_async_engine,
    get_engine,
)
from .search import hybrid_search, ReciprocalRankFuser, ScoredHit
from .types import CatalogSearchResult, ContentSearchResult

# shared code
from vector_indexing.components.embedders.google import GoogleEmbedder
from vector_indexing.components.backends.turbopuffer import TurbopufferBackend
from vector_indexing.core.utils import Timer
from logger import create_log
from utils.common import require_env, wrap
from utils.timer import timer

# api code
from ..utils import remove_markdown_comments
from .search import ReciprocalRankFuser, ScoredHit, hybrid_search
from .types import CatalogSearchResult, ContentSearchResult
from ..newrelic_llm_events import record_llm_events
from ..db import (
    get_async_engine,
    get_engine,
    get_frbr_data_by_edition,
    get_readonly_session,
)

logger = create_log(__name__)

# max number of editions to return from catalog search
PAGE_SIZE = 10

PROMPTS_DIR = Path(__file__).parent / "prompts"


## Turbopuffer filter parsing

# Attributes that may be incomplete/null in the index
INCOMPLETE_ATTRIBUTES = {"subject", "language", "publication_date", "author"}

# Attributes that should be converted to datetime objects
DATETIME_ATTRIBUTES = {"publication_date"}

# Meta-operators that wrap child filters
META_OPERATORS = {"And", "Or", "Not"}


def dynamic_docstring(docstring):
    """Decorator to set a function's docstring dynamically."""

    def decorator(func):
        func.__doc__ = docstring
        return func

    return decorator


def transform_datetime(filter_array: Any) -> List:
    """
    Processing function to convert datetime string values to datetime objects.
    Only processes 3-element filters with datetime fields.

    Args:
        filter_array: Filter specification to potentially process

    Returns:
        Modified filter with converted datetime value if applicable, or filter_array
        unchanged if the filter does not meet the criteria for conversion.

    Raises:
        ValueError: If datetime conversion fails
    """
    # Only process 3-element condition lists [attribute, operator, value]
    if not (isinstance(filter_array, (list, tuple)) and len(filter_array) == 3):
        return filter_array

    field_name, operator, value = filter_array

    # simple filter arrays always start with 2 strings
    # Note: value is either string or array of string for our index
    if not (isinstance(field_name, str) and isinstance(operator, str)):
        return filter_array

    # Only datetime fields eligible for conversion
    if field_name not in DATETIME_ATTRIBUTES:
        return filter_array

    # Only valid non-string value is `None`, pass through unchanged
    if value is None:
        return filter_array

    # Convert datetime string values
    try:
        # Parse ISO 8601 date string to datetime
        # NOTE: handles py <3.11 quirk were Z isn't handled, although timezone should never be used here
        converted_value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return [field_name, operator, converted_value]
    except Exception as e:
        raise ValueError(
            f"Failed to convert '{value}' to datetime for field '{field_name}'"
        ) from e


def transform_incomplete(filter_array: Any) -> List:
    """
    Wrap filter with null-matching OR condition if attribute has incomplete data.

    Args:
        filter_array: Filter specification to potentially process

    Returns:
        Filter wrapped in Or condition with null match if applicable, or filter_array
        unchanged if the filter does not meet the criteria for transformation.
    """
    # Only process 3-element condition lists [attribute, operator, value]
    if not isinstance(filter_array, (list, tuple)) or len(filter_array) != 3:
        return filter_array

    field_name, operator, value = filter_array

    # Only process incomplete attributes
    if field_name not in INCOMPLETE_ATTRIBUTES:
        return filter_array

    return [
        "Or",
        [
            [field_name, operator, value],  # Original filter
            [field_name, "Eq", None],  # Null match
        ],
    ]


def recurse_filters(filter_: Any, processing_func: Callable) -> Any:
    """
    Generic recursive post-processor for TurboPuffer style (nestable) filters
    that applies a processing function to every simple (leaf) filter.

    Recursion is driven by filter structure:
    - Meta-operators (And, Or, Not) → recurse into child filters
    - Everything else → treat as a simple filter and pass to processing_func

    The processing function must return the filter unchanged if its conditions
    are not met.

    Args:
        filter_: A complete filter specification (list/tuple). Scalars are invalid
                 and will raise ValueError.
        processing_func: Function that takes a simple filter and returns either
                         a transformed filter or the original filter unchanged.

    Returns:
        Processed filter with transformations applied where applicable

    Raises:
        ValueError: If filters is not a list or tuple
    """
    if not isinstance(filter_, (list, tuple)):
        raise ValueError(
            f"Expected filter to be a list or tuple, got {type(filter_).__name__}: {filter_!r}"
        )

    if len(filter_) == 0:
        raise ValueError("Filter cannot be an empty list or tuple")

    operator = filter_[0]

    if operator in META_OPERATORS:
        if operator == "Not":
            # ["Not", child_filter]
            return [operator, recurse_filters(filter_[1], processing_func)]
        else:
            # ["And"/"Or", [child_filter, ...]]
            return [
                operator,
                [recurse_filters(child, processing_func) for child in filter_[1]],
            ]

    # Simple filter: [attribute, operator, value] — pass whole filter to processing_func.
    # processing_func returns the filter unchanged if its conditions are not met.
    return processing_func(filter_)


def apply_filter_transforms(filters: Any, apply_null_matching: bool = True) -> Any:
    """
    Apply all filter post-processing transformations in sequence.

    This provides a modular, extensible pipeline for filter transformations.
    New post-processing steps can be added here as needed.

    Args:
        filters: Raw filter specification
        apply_null_matching: Whether to add null matching for incomplete attributes

    Returns:
        Processed filters with all transformations applied
    """
    if filters is None:
        return None

    # Step 1: Convert datetime strings to datetime objects
    filters = recurse_filters(filters, processing_func=transform_datetime)

    # Step 2: Add null matching for incomplete attributes (if enabled)
    if apply_null_matching:
        filters = recurse_filters(filters, processing_func=transform_incomplete)

    # MAYBE: post process to check that null isn't passed as a string

    logger.debug(f"Post-processed filters: {filters}")
    return filters


@dataclass
class CatalogSearchExecutionContext:
    """Container used to inject objects into each agent run execution."""

    backend: TurbopufferBackend
    embedder: GoogleEmbedder
    session_id: str
    conversation_type: str = "catalogSearch"
    search_results: Dict = field(default_factory=dict)


@dataclass
class ContentSearchExecutionContext:
    """Container used to inject objects into each agent run execution.

    Either edition_id or barcode must be provided. When barcode is set, the
    book filter for Turbopuffer searches is applied on `barcode` (stable across
    FRBR re-clustering); otherwise it falls back to filtering on `edition_id`.
    """

    backend: TurbopufferBackend
    embedder: GoogleEmbedder
    session_id: str
    edition_id: int
    barcode: Optional[str] = None
    conversation_type: str = "contentSearch"
    search_results: Dict = field(default_factory=dict)
    frbr_fields: Dict = field(default_factory=dict)


# TODO: make a name and args callback for any tool, and add a edition_id log message to search_book, add traceback log to this callback
# also time tool call construction latency
class LLMLoggingHooks(RunHooks):
    """Agent lifecycle hooks that log LLM call start/end with timing and response."""

    def __init__(
        self, session_id: str, conversation_type: str, edition_id: Optional[int] = None
    ):
        self.session_id = session_id
        self.conversation_type = conversation_type
        self.edition_id = edition_id

        self._llm_start_time: Optional[float] = None
        self._tool_start_time: Optional[float] = None

        self.num_llm_calls = 0
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_llm_elapsed = 0.0

        self._last_system_prompt: Optional[str] = None
        self._last_input_items: list = []
        self._last_response: Optional[ModelResponse] = None

    async def on_llm_start(
        self,
        context: RunContextWrapper,
        agent: Agent,
        system_prompt: Optional[str],
        input_items: list,
    ) -> None:
        self._llm_start_time = time.perf_counter()

        self._last_system_prompt = system_prompt
        self._last_input_items = input_items

    async def on_llm_end(
        self,
        context: RunContextWrapper,
        agent: Agent,
        response: ModelResponse,
    ) -> None:
        elapsed = (
            time.perf_counter() - self._llm_start_time
            if self._llm_start_time is not None
            else 0.0
        )
        self._llm_start_time = None

        self.num_llm_calls += 1
        self.total_llm_elapsed += elapsed
        self._last_response = response

        if response.usage:
            self.total_input_tokens += response.usage.input_tokens or 0
            self.total_output_tokens += response.usage.output_tokens or 0

        elapsed_str = f"{elapsed:.3f}s"
        output_types = [o.type for o in response.output] if response.output else []
        logger.info(
            f"LLM response received | elapsed: {elapsed_str} | output types: {output_types} | token usage: input={response.usage.input_tokens} output={response.usage.output_tokens}"
        )

    async def on_tool_start(
        self,
        context: RunContextWrapper,
        agent: Agent,
        tool,
    ) -> None:
        self._tool_start_time = time.perf_counter()
        # logger.info(f"Tool starting | tool: {tool.name}")

    async def on_tool_end(
        self,
        context: RunContextWrapper,
        agent: Agent,
        tool,
        result: str,
    ) -> None:
        elapsed = (
            time.perf_counter() - self._tool_start_time
            if self._tool_start_time is not None
            else None
        )
        self._tool_start_time = None
        elapsed_str = f"{elapsed:.3f}s" if elapsed is not None else "unknown"
        logger.info(
            f"Tool execution completed | tool: {tool.name} | elapsed: {elapsed_str}"
        )


@timer(logger)
def map_editions_and_records(record_ids=None, edition_ids=None):
    if record_ids:
        ids = record_ids
        source_col = "record_id"
        target_col = "edition_id"
    elif edition_ids:
        assert record_ids is None, "both record_ids and edition_ids are non-null"
        ids = edition_ids
        source_col = "edition_id"
        target_col = "record_id"

    logger.debug(f"Mapping {source_col}s to {target_col}s...")

    # Use UNNEST with a CTE instead of expanding IN clause for better performance
    # with large ID lists. DISTINCT ON deduplicates at the DB level.
    if source_col == "record_id":
        query = text("""
            WITH requested(id) AS (
                SELECT UNNEST(CAST(:ids AS INTEGER[]))
            )
            SELECT DISTINCT ON (r.id)
                r.id AS record_id,
                i.id AS item_id,
                e.id AS edition_id
            FROM requested
            JOIN records r ON r.id = requested.id
            JOIN items i ON r.id = i.record_id
            JOIN editions e ON i.edition_id = e.id
            ORDER BY r.id
        """)
    else:
        query = text("""
            WITH requested(id) AS (
                SELECT UNNEST(CAST(:ids AS INTEGER[]))
            )
            SELECT DISTINCT ON (e.id)
                r.id AS record_id,
                i.id AS item_id,
                e.id AS edition_id
            FROM requested
            JOIN editions e ON e.id = requested.id
            JOIN items i ON e.id = i.edition_id
            JOIN records r ON i.record_id = r.id
            ORDER BY e.id
        """)

    Session = get_readonly_session()
    with Session() as session:
        result = session.execute(query, {"ids": list(ids)})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    logger.debug(
        f"Successfully mapped {len(df)} {source_col}s to {df[target_col].unique().size} {target_col}s"
    )

    # Create dict mapping: source id -> {target id -> value, ...}
    return df.set_index(source_col).to_dict(orient="index")


_MAX_TURNS_SYSTEM_PROMPT = """\
You are a research library assistant. You have reached the maximum number of \
allowed agent turns in the agent loop. 

Here is the original system prompt:
```
{agent_system_prompt}
```

Given the conversation context, deliver a polite response according to your \
system prompt with the partially available information if possible or an \
appropriate apology otherwise — If the search tool returned an error acknowledge \
that the search was not fully complete. \
"""


async def _on_max_turns(data: RunErrorHandlerInput) -> RunErrorHandlerResult:
    """Handles assistant response if max agent turns are exceeded."""
    client: AsyncOpenAI = data.run_data.last_agent.model._client
    model_name: str = data.run_data.last_agent.model.model
    agent_system_prompt = await data.run_data.last_agent.get_system_prompt(data.context)

    messages = [
        {
            "role": "system",
            "content": _MAX_TURNS_SYSTEM_PROMPT.format(
                agent_system_prompt=agent_system_prompt
            ),
        },
        *Converter.items_to_messages(data.run_data.history, model=model_name),
    ]

    response = await client.chat.completions.create(
        model=model_name,
        messages=messages,
        temperature=0.0,
        reasoning_effort=None,
    )

    logger.warning("update_chat: MaxTurnsExceeded — graceful final response generated.")

    return RunErrorHandlerResult(
        final_output=response.choices[0].message.content,
        include_in_history=True,
    )


@timer(logger)
async def update_chat(
    message: str,
    conversation_type: str,
    session_id: str,
    edition_id=None,
    barcode=None,
    max_turns: int = DEFAULT_MAX_TURNS,
) -> RunResult:
    """
    Send a message to the conversation and get the agent's response.

    Conversation history is managed server-side via SQLAlchemySession keyed on
    session_id. The caller sends only the new user message; the SDK loads prior
    turns from the database automatically.

    The raw search results will be available in self.context.search_data
    for any post-processing or enrichment needed.

    Args:
        message: The new user message text.
        conversation_type: Either "contentSearch" or "catalogSearch" to pick the search mode.
        session_id: Client-supplied session ID. History is persisted to and loaded
                    from the database using this key.
        edition_id: Identifies the book for content search. Either this or
            ``barcode`` must be provided when conversation_type is
            "contentSearch". If both are provided, ``barcode`` takes
            precedence (it is stable across FRBR re-clustering).

    Returns:
        The agent's RunResult obj.
    """

    # TODO: when a user switches from catalog to content search, we should add \
    # an additional user message saying: "I am now switching to content search \
    # with in book XYZ" or "I am now switching back to catalog search"

    # TODO: figure out how to do thread safe  module level instantiations for \
    # some reused objs (backend, system prompts, async loop, etc...) (for sharing btw server \
    # request workers/threads)

    backend = TurbopufferBackend(index_name=require_env("TURBOPUFFER_NAMESPACE"))
    embedder = GoogleEmbedder()

    # NOTE: litellm has a bug converting `list | None = None` in agents sdk @functol_tool
    # param type annotations into gemini API compatible format

    # model = "litellm/gemini/gemini-3-flash-preview"
    model = OpenAIChatCompletionsModel(
        model="gemini-3-flash-preview",
        # model="gemini-3.5-flash",
        openai_client=AsyncOpenAI(
            api_key=require_env("GOOGLE_API_KEY"),
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        ),
    )

    system_prompt_template = Template(
        (PROMPTS_DIR / "system" / "1.jinja.md").read_text()
    )

    # Search within single book
    if conversation_type == "contentSearch":
        # Fetch FRBR data for the book
        with Timer(
            "get_frbr_data_by_edition",
            on_exit=lambda name, elapsed: logger.info(
                f"{name} took {elapsed:.3f}s for 1 edition"
            ),
        ):
            frbr_data = get_frbr_data_by_edition([edition_id])
        if not frbr_data:
            logger.error(
                f"FRBR data missing for content search in edition {edition_id}"
            )
            raise ValueError(f"No edition found with id {edition_id}")
        # Prefer barcode-based lookup when provided. Barcode is stable across
        # FRBR re-clustering, whereas edition_id is not.
        if barcode is not None:
            with Timer(
                "get_frbr_data_by_barcode",
                on_exit=lambda name, elapsed: logger.info(
                    f"{name} took {elapsed:.3f}s for 1 barcode"
                ),
            ):
                frbr_data = get_frbr_data_by_barcode([barcode])
            if not frbr_data:
                logger.error(
                    f"FRBR data missing for content search by barcode {barcode}"
                )
            resolved_edition_id = frbr_data[0].Edition.id if frbr_data else None
        else:
            with Timer(
                "get_frbr_data_by_edition",
                on_exit=lambda name, elapsed: logger.info(
                    f"{name} took {elapsed:.3f}s for 1 edition"
                ),
            ):
                frbr_data = get_frbr_data_by_edition([edition_id])
            if not frbr_data:
                logger.error(
                    f"FRBR data missing for content search in edition {edition_id}"
                )
            resolved_edition_id = edition_id
        frbr_fields = format_frbr_fields(frbr_data[0].Work, frbr_data[0].Edition)

        exec_context = ContentSearchExecutionContext(
            backend=backend,
            embedder=embedder,
            edition_id=resolved_edition_id,
            session_id=session_id,
            barcode=barcode,
            frbr_fields=frbr_fields,
        )

        system_prompt = system_prompt_template.render(
            conversation_type="contentSearch", frbr_fields=frbr_fields
        )
        tools = [search_book]

    # Search for books in catalog
    else:  # conversation_type == "catalogSearch":
        exec_context = CatalogSearchExecutionContext(
            backend=backend, embedder=embedder, session_id=session_id
        )
        system_prompt = remove_markdown_comments(
            system_prompt_template.render(conversation_type="catalogSearch")
        )
        tools = [search_catalog]

    agent = Agent[type(exec_context)](
        name="Research Library Assistant",
        model=model,
        instructions=system_prompt,
        tools=tools,
    )

    session = SQLAlchemySession(session_id, engine=get_async_engine())

    # Add conversation metadata to New Relic transaction for grouping and filtering
    newrelic.agent.add_custom_attribute("llm.conversation_id", session_id)
    newrelic.agent.add_custom_attribute("llm.conversation_type", conversation_type)
    newrelic.agent.add_custom_attribute(
        "llm.edition_id", edition_id if conversation_type == "contentSearch" else None
    )

    logging_hooks = LLMLoggingHooks(
        session_id=session_id,
        conversation_type=conversation_type,
        edition_id=edition_id if conversation_type == "contentSearch" else None,
    )

    # Wrap agent exec loop in NR APM trace for AI monitoring dashboard compatibility
    with newrelic.agent.FunctionTrace(name="create", group="Llm/completion/OpenAI"):
        run_result = await Runner.run(
            starting_agent=agent,
            input=message,
            context=exec_context,
            hooks=logging_hooks,
            max_turns=max_turns,
            error_handlers={"max_turns": _on_max_turns},
            session=session,
            run_config=RunConfig(
                tracing_disabled=True,
                model_settings=ModelSettings(
                    temperature=0.0,
                    reasoning=Reasoning(effort="none"),
                    include_usage=True,
                ),
            ),
        )

    record_llm_events(
        agent,
        session_id=logging_hooks.session_id,
        conversation_type=logging_hooks.conversation_type,
        edition_id=logging_hooks.edition_id,
        last_response=logging_hooks._last_response,
        last_system_prompt=logging_hooks._last_system_prompt,
        last_input_items=logging_hooks._last_input_items,
        total_input_tokens=logging_hooks.total_input_tokens,
        total_output_tokens=logging_hooks.total_output_tokens,
        total_llm_elapsed=logging_hooks.total_llm_elapsed,
    )

    return run_result


def get_session_messages(session_id):
    """Read message data for session ID as ND-JSON"""
    # TODO: replace with Session.get_items()
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM agent_messages WHERE session_id = :sid ORDER BY id"),
            {"sid": session_id},
        ).fetchall()
    messages = [json.loads(row.message_data) for row in rows]
    return messages


def delete_session_data(session_id: str) -> None:
    """Delete all rows in agent_messages and agent_sessions for the given session_id."""
    # TODO: replace with Session.clear_session()
    engine = get_engine()
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(
                text("DELETE FROM agent_messages WHERE session_id = :sid"),
                {"sid": session_id},
            )
            conn.execute(
                text("DELETE FROM agent_sessions WHERE session_id = :sid"),
                {"sid": session_id},
            )


def max_chunk_score(chunk_hits):
    return max([h["score"] for h in chunk_hits])


def min_chunk_score(chunk_hits):
    return min([h["score"] for h in chunk_hits])


def mean_chunk_score(chunk_hits):
    return np.mean([h["score"] for h in chunk_hits])


def results_to_chunk_hits(results: list[ScoredHit]) -> Iterator[dict[str, Any]]:
    """
    Yield chunk_hit's from search index search results, adding item_id to each
    chunk_hit by mapping chunk record_id to item_id in DB.

    Args:
        results: List of (ChunkDocument, score) tuples
    """
    # chunk_hit = ChunkDocument + score (+ item_id)
    # NOTE: future: the item_id will be directly indexed in the chunk hit \
    # making this function unnecessary, ScoredHit can be used instead.

    missing_item_ids = []
    try:
        # map record_id->item_id to add to chunk metadata
        # book_id = record_id
        record_ids = set(cd.book_id for cd, _ in results)
        mapper = map_editions_and_records(record_ids=record_ids)

        # Results from hybrid_search are (ChunkDocument, rrf_score) tuples
        for chunk_doc, rrf_score in results:
            chunk_hit = chunk_doc.to_dict()
            chunk_hit["score"] = rrf_score if rrf_score is not None else 0.0
            # book_id was incorrectly indexed as a str
            item_id = mapper.get(int(chunk_hit["book_id"]), {}).get("item_id")
            if item_id is None:
                missing_item_ids.append(chunk_hit["book_id"])
                continue
            chunk_hit["item_id"] = item_id
            yield chunk_hit
    finally:
        if missing_item_ids:
            logger.error(
                f"These {len(set(missing_item_ids))} record_ids do not map to an item_id: {set(missing_item_ids)}"
            )


# TODO: make score type metadata of the chunk index search method
CHUNK_SCORE_TYPE: Literal["higher-is-better", "lower-is-better"] = "higher-is-better"

if CHUNK_SCORE_TYPE == "higher-is-better":
    SCORE_AGGREGATOR = max_chunk_score
    SCORE_SORT_DIRECTION = {"reverse": True}
    SCORE_LABEL = "MAX SCORE"
else:
    SCORE_AGGREGATOR = min_chunk_score
    SCORE_SORT_DIRECTION = {"reverse": False}
    SCORE_LABEL = "MIN SCORE"


# Module-level docstring variables
SEARCH_CATALOG_DOC = f"""
{(PROMPTS_DIR / "tools" / "search_catalog.txt").read_text()}

{remove_markdown_comments((PROMPTS_DIR / "tools" / "tool.md").read_text())}
"""


@function_tool
@dynamic_docstring(SEARCH_CATALOG_DOC)
def search_catalog(
    ctx: ToolContext[CatalogSearchExecutionContext],
    ranking_query: str,
    filters: List | tuple | None = None,
    filters_match_null: bool = True,
) -> str:
    try:
        logger.info(f"{ctx.tool_name} tool called with args: '{ctx.tool_arguments}'")

        # Post-process filters through the pipeline
        filters = apply_filter_transforms(
            filters, apply_null_matching=filters_match_null
        )

        # Embed the query for semantic search
        query_vector = ctx.context.embedder.embed_query(ranking_query)

        # Execute hybrid search (vector + BM25) with RRF
        results = hybrid_search(
            backend=ctx.context.backend,
            query_vector=query_vector,
            ranking_query=ranking_query,
            top_k=100,
            filters=filters,
            fuser=ReciprocalRankFuser(k=60),
        )
        logger.info(f"Retrieved {len(results)} chunk hits from hybrid search")

        if not len(results):
            return "No results found for your query."

        # MAYBE: turn the below into 2 functions: group_by_edition_and_sort() and enrich_edition_hits() (with limit to top 10 in between)

        # Group chunk hits by barcode (stable across FRBR re-clustering).
        # The DB-resolved edition_id is attached after the FRBR fetch below.
        # edition_hit = {"chunk_hits", "agg_score", "barcode"}
        edition_hits = {}
        for chunk_hit in results_to_chunk_hits(results):
            edition_hits.setdefault(
                chunk_hit["barcode"],
                {"barcode": chunk_hit["barcode"], "chunk_hits": []},
            )["chunk_hits"].append(chunk_hit)

        # Calculate aggregate edition score
        edition_hits = [
            {**eh, "agg_score": SCORE_AGGREGATOR(eh["chunk_hits"])}
            for eh in edition_hits.values()
        ]
        logger.info(
            f"Aggregated {len(edition_hits)} editions from {len(results)} chunk hits"
        )

        # TODO: if fewer than PAGE_SIZE editions, re-query more chunks until PAGE_SIZE editions are retrieved

        # NOTE: RRF scores are higher-is-better, unlike raw ANN distances (lower-is-better).
        # This sort order change (reverse=True) was intentional and also fixes a pre-existing
        # inconsistency between ES9 and Turbopuffer score semantics.
        edition_hits = sorted(
            edition_hits, key=lambda eh: eh["agg_score"], **SCORE_SORT_DIRECTION
        )

        # Limit results to the 10 top scoring editions
        if len(edition_hits) > PAGE_SIZE:
            logger.info(
                f"Limiting results to first {PAGE_SIZE} editions sorted by '{CHUNK_SCORE_TYPE}'"
            )
            edition_hits = edition_hits[:PAGE_SIZE]
            # TODO: handle paginating or providing more edition hits

        # Fetch FRBR data (from DB) by barcode. Barcode is stable across FRBR
        # re-clustering, while the edition_id stored on chunk hits in TP may
        # be stale; the DB-resolved edition_id (highest per barcode = most
        # recently clustered) is what we attach to results for the FE.
        # TODO: remove fetch of DB data everything the LLM needs is in TP \
        # (right?) and the FE fetches other metadata in a separate request.
        barcodes = [h["barcode"] for h in edition_hits]
        logger.info(f"Fetching FRBR metadata for the following barcodes: {barcodes}")
        with Timer(
            "get_frbr_data_by_barcode",
            on_exit=lambda name, elapsed: logger.info(
                f"{name} took {elapsed:.3f}s for {len(barcodes)} barcodes"
            ),
        ):
            frbr_data = get_frbr_data_by_barcode(barcodes)

        # Merge ES hit data and FRBR metadata (maintaining edition sort order)
        frbr_data = {row.barcode: row for row in frbr_data}
        edition_data = []  # list of EditionResult
        missing_data = []
        for edition_hit in edition_hits:
            # match DB orm work/edition to vector search edition hit by barcode
            row = frbr_data.get(edition_hit["barcode"])
            if row is None:
                missing_data.append(edition_hit["barcode"])
            else:
                edition_data.append(
                    CatalogSearchResult(
                        orm_work=row.Work,
                        orm_edition=row.Edition,
                        edition_id=row.Edition.id,
                        chunk_hits=edition_hit["chunk_hits"],
                        agg_score=edition_hit["agg_score"],
                    )
                )
        if missing_data:
            logger.error(
                f"Missing Data: {len(missing_data)} barcodes from vector search results have no matching data in DB: {missing_data}"
            )
            logger.info(
                f"Limiting results to the {len(edition_data)} editions that have metadata in the DB."
            )

        # NOTE: the biggest difference btw VRA (current state) search and DRB \
        # search (in terms of output formatting) is that VRA search does FRBR obj \
        # sorting outside/after ES and does not purely rely on ES search to \
        # determine results ordering (because direct results are grouped by \
        # edition outside of ES  in VRA)

        # Store search results for later reference
        ctx.context.search_results[ctx.tool_call_id] = {
            "tool_name": ctx.tool_name,
            "edition_data": edition_data,  # ordered search result
            "search_params": json.loads(ctx.tool_arguments),
        }

        # Format editions for LLM (markdown)
        # ALT : convert edition data to json and send (full) JSON to LLM (simpler \
        # than saving JSON/API response separately but edition data json may \
        # include irrelevant metadata)
        return format_search_results(edition_data, as_str=True)

    except Exception as e:
        logger.exception(f"Error during {ctx.tool_name} tool execution.")
        raise e


SEARCH_BOOK_DOC = f"""
{(PROMPTS_DIR / "tools" / "search_book.txt").read_text()}

{remove_markdown_comments((PROMPTS_DIR / "tools" / "tool.md").read_text())}
"""


@function_tool
@dynamic_docstring(SEARCH_BOOK_DOC)
def search_book(
    ctx: ToolContext[ContentSearchExecutionContext],
    ranking_query: str,
    filters: Optional[Union[List, tuple]] = None,
    filters_match_null: bool = True,
) -> str:
    try:
        logger.info(
            f"{ctx.tool_name} tool called with args: '{ctx.tool_arguments}', "
            f"for edition_id = {ctx.context.edition_id}, barcode = {ctx.context.barcode}"
        )

        # Post-process filters through the pipeline
        filters = apply_filter_transforms(
            filters, apply_null_matching=filters_match_null
        )

        # Build filter to restrict search to single book. Prefer barcode
        # (stable identifier) when available; fall back to edition_id.
        if ctx.context.barcode is not None:
            book_filter = ["barcode", "Eq", ctx.context.barcode]
        else:
            book_filter = ["edition_id", "Eq", ctx.context.edition_id]

        # Combine with user filters if provided
        if filters is not None:
            combined_filters = ["And", [book_filter, filters]]
        else:
            combined_filters = book_filter

        # Embed the query for semantic search
        query_vector = ctx.context.embedder.embed_query(ranking_query)

        # Execute hybrid search (vector + BM25) with RRF fusion
        results = hybrid_search(
            backend=ctx.context.backend,
            query_vector=query_vector,
            ranking_query=ranking_query,
            top_k=10,
            filters=combined_filters,
        )
        logger.info(f"Retrieved {len(results)} chunk hits from hybrid search for book")

        if not len(results):
            return "No results found for your query in this book."

        chunk_hits = list(results_to_chunk_hits(results))

        # Store search results for later reference
        ctx.context.search_results[ctx.tool_call_id] = {
            "tool_name": ctx.tool_name,
            "edition_data": [
                ContentSearchResult(
                    edition_id=ctx.context.edition_id,
                    chunk_hits=chunk_hits,
                    frbr_fields=ctx.context.frbr_fields,
                )
            ],
            "search_params": json.loads(ctx.tool_arguments),
        }

        # Format results for LLM
        return format_search_results(
            ctx.context.search_results[ctx.tool_call_id]["edition_data"],
            as_str=True,
        )

    except Exception as e:
        logger.exception(f"Error during {ctx.tool_name} tool execution.")
        raise e


def format_frbr_fields(orm_work, orm_edition):
    """
    Format ORM work and edition attributes for printing.
    """
    title = orm_work.title or "(Title Unavailable)"

    authors = orm_work.authors or []
    authors_concat = (
        ", ".join([a.get("name", "") for a in authors if isinstance(a, dict)])
        if authors
        else "(Authors Unavailable)"
    )

    subjects = orm_work.subjects or []
    subjects_concat = (
        ", ".join([s.get("heading", "") for s in subjects if isinstance(s, dict)])
        if subjects
        else "(Subjects Unavailable)"
    )

    pub_date = (
        str(orm_edition.publication_date)
        if orm_edition.publication_date
        else "(Publication Date Unavailable)"
    )

    languages = orm_edition.languages or []
    languages_concat = (
        ", ".join(
            [
                lang.get("language", "") if isinstance(lang, dict) else str(lang)
                for lang in languages
            ]
        )
        if languages
        else "(Languages Unavailable)"
    )

    # NOTE: publisher is the only field not indexed in TP (only in DB)
    publishers = orm_edition.publishers or []
    publishers_concat = (
        ", ".join([p.get("name", "") for p in publishers if isinstance(p, dict)])
        if publishers
        else "(Publishers Unavailable)"
    )

    return {
        "title": title,
        "author_names": authors_concat,
        "subject_list": subjects_concat,
        "pub_date": pub_date,
        "publisher_names": publishers_concat,
        "language_list": languages_concat,
    }


def display_book(lines, frbr_fields, chunk_hits, edition_id):
    """
    Create lines of str for an XML display of book and chunk search results.
    Chunk display order controlled by input data order.
    """
    lines.append("\n<edition>")
    # Display book level metadata

    # MAYBE: edition index not id?
    lines.append(f"<edition_id>{edition_id}</edition_id>")
    lines.append(f"<title>{frbr_fields['title']}</title>")
    lines.append(f"<authors>{frbr_fields['author_names']}</authors>")
    lines.append(f"<publisher>{frbr_fields['publisher_names']}</publisher>")
    lines.append(f"<date>{frbr_fields['pub_date']}</date>")
    lines.append(f"<subjects>{frbr_fields['subject_list']}</subjects>")
    lines.append(f"<language>{frbr_fields['language_list']}</language>")
    # MAYBE: add agg_score
    # MAYBE: print the number of chunks per edition somehow

    # Display chunk level information
    lines.append("<chunks>")

    # MAYBE: sort chunks by score (bigger is better, missing scores last) and limit display
    for chunk_hit in chunk_hits:
        text = chunk_hit.get("text", "(No Text)")
        # Extract page range from chunk metadata (supports both formats)
        start_page = chunk_hit.get("start_page") or chunk_hit.get("chunk_start_page")
        end_page = chunk_hit.get("end_page") or chunk_hit.get("chunk_end_page")
        if start_page is not None and end_page is not None:
            if start_page == end_page:
                page_display = str(start_page)
            else:
                page_display = f"{start_page}-{end_page}"
        else:
            page_display = "?"

        lines.append("\n<chunk>")
        # MAYBE: add chunk index? to tag?
        # MAYBE: chunk score?
        lines.append(
            f"<item_id>{chunk_hit['item_id']}</item_id>"
        )  # an edition might include chunks from multiple items
        lines.append(f"<page>{page_display}</page>")
        lines.append(f"<text>\n{text}\n</text>")
        lines.append("</chunk>")

    lines.append("\n</chunks>")
    lines.append("</edition>")

    return lines


# MAYBE: remove book level info from search response for contentSearch to save tokens.
def format_search_results(
    edition_data, search_tool_call_id=None, query=None, as_str=False
):
    """
    Print or return a formatted str containing an ordered list of editions and their
    associated text excerpts. For each edition, metadata (title, authors, subjects,
    publication date) and chunk text excerpts with page numbers are displayed.
    Editions are ordered by the input list.

    Args:
        edition_data: List of BaseEditionResult (CatalogSearchResult or ContentSearchResult)
        search_tool_call_id: Optional tool call ID to include in output header
        query: The search query string
        as_str: If True, return as string; otherwise print
    """
    if not edition_data:
        return "There are no results for your query."

    lines = []
    lines.append("<search_results>")

    if query is not None:
        lines.append(f"<query>{wrap(query)}</query>")

    if search_tool_call_id is not None:
        lines.append(
            f"<search_tool_call_id>{search_tool_call_id}</search_tool_call_id>"
        )

    for entry in edition_data:
        frbr_fields = (
            format_frbr_fields(entry.orm_work, entry.orm_edition)
            if isinstance(entry, CatalogSearchResult)
            else entry.frbr_fields
        )
        lines = display_book(lines, frbr_fields, entry.chunk_hits, entry.edition_id)

    lines.append("\n</search_results>")

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


# UNUSED
def compact_display_editions(edition_data, query, as_str=False):
    """
    Display edition search results in compact format.

    Args:
        edition_data: List of EditionResult containing 'orm_work', 'orm_edition', 'edition_hit'
        query: The search query string
        as_str: If True, return as string; otherwise print
    """
    if not edition_data:
        return "There are no results for your query."

    lines = []
    lines.append(f'QUERY: "{wrap(query)}"')
    lines.append("RESULTS:")

    for i, edition_entry in enumerate(edition_data, 1):
        orm_work = edition_entry.orm_work
        orm_edition = edition_entry.orm_edition
        title = orm_work.title or "(No Title)"
        chunk_hits = edition_entry.chunk_hits

        # Truncate title if too long
        title_display = title[:60] + "..." if len(title) > 60 else title

        lines.append(
            f" {i:>3}:  ({edition_entry.agg_score:.3f}) Ed:{orm_edition.id:<6} W:{orm_work.id:<6} [{len(chunk_hits)} chunks] - {title_display}"
        )

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


# UNUSED
def compact_display_chunks(chunk_hits, query, as_str=False):
    # Sort entries by ['meta']['score'] descending, missing scores last
    sorted_entries = sorted(chunk_hits, key=get_score, reverse=True)

    lines = []
    lines.append(f'QUERY: "{wrap(query)}"')
    lines.append("RESULTS:")
    for i, entry in enumerate(sorted_entries, 1):
        lines.append(
            f" {i:>3}:  ({get_score(entry):.3f}) {entry['doc_id']:<19} -  {entry['title']}"
        )

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)
