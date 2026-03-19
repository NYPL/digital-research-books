from dataclasses import dataclass, field, asdict
from datetime import datetime
import difflib
import json
from pathlib import Path
import re
import traceback
import asyncio
from typing import Dict, Any, Literal, Optional, Union, List, Iterator, Callable, Tuple
from typing_extensions import TypedDict
from enum import Enum
import uuid
from textwrap import indent
import sys
import os
import asyncio
import time

import numpy as np
import pandas as pd
from agents import (
    Agent,
    RunHooks,
    OpenAIChatCompletionsModel,
    Runner,
    RunConfig,
    function_tool,
    RunContextWrapper,
    SQLiteSession,
    ModelSettings,
    RunResult,
)
from agents.items import ModelResponse, ToolCallOutputItem
from agents.tool_context import ToolContext
from agents.extensions.memory import SQLAlchemySession
from openai import AsyncOpenAI
from openai.types.shared import Reasoning
from pydantic import BaseModel
from sqlalchemy import text
from jinja2 import Template
from rapidfuzz import fuzz
import rapidfuzz


# from sqlalchemy.ext.asyncio import create_async_engine

# api code
from ..utils import APIUtils, hit_to_dict, remove_markdown_comments, shorten
from ..db import get_frbr_data_by_edition, get_session

# shared code
from vector_indexing.components.embedders.google import GoogleEmbedder
from vector_indexing.components.backends.turbopuffer import TurbopufferBackend
from vector_indexing.core.config import get_config
from vector_indexing.core.utils import Timer
from logger import create_log
from utils.common import wrap, require_env
from utils.timer import timer

# hybrid search
from .search import hybrid_search, ReciprocalRankFuser, ScoredHit


logger = create_log(__name__)

# max number of editions to return from catalog search
PAGE_SIZE = 10

INDEX_NAME = "vra-dev"

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


# Module-level docstring variables
SEARCH_CATALOG_DOC = f"""
{(PROMPTS_DIR / "tools" / "search_catalog.txt").read_text()}

{remove_markdown_comments((PROMPTS_DIR / "tools" / "tool.md").read_text())}
"""

SEARCH_BOOK_DOC = f"""
{(PROMPTS_DIR / "tools" / "search_book.txt").read_text()}

{remove_markdown_comments((PROMPTS_DIR / "tools" / "tool.md").read_text())}
"""

VALIDATE_RELEVANT_SNIPPETS_DOC = (
    PROMPTS_DIR / "tools" / "select_relevant_snippets.txt"
    # PROMPTS_DIR / "tools" / "select_relevant_snippets_no_elision.txt"
).read_text()


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


def recurse_filters(filters: Any, processing_func: Callable) -> Any:
    """
    Generic recursive post-processor for TurboPuffer style filters that applies
    a processing function to every simple (leaf) filter.

    Recursion is driven by filter structure:
    - Meta-operators (And, Or, Not) → recurse into child filters
    - Everything else → treat as a simple filter and pass to processing_func

    The processing function must return the filter unchanged if its conditions
    are not met.

    Args:
        filters: A complete filter specification (list/tuple). Scalars are invalid
                 and will raise ValueError.
        processing_func: Function that takes a simple filter and returns either
                         a transformed filter or the original filter unchanged.

    Returns:
        Processed filters with transformations applied where applicable

    Raises:
        ValueError: If filters is not a list or tuple
    """
    if not isinstance(filters, (list, tuple)):
        raise ValueError(
            f"Expected filter to be a list or tuple, got {type(filters).__name__}: {filters!r}"
        )

    if len(filters) == 0:
        raise ValueError("Filter cannot be an empty list or tuple")

    operator = filters[0]

    if operator in META_OPERATORS:
        if operator == "Not":
            # ["Not", child_filter]
            return [operator, recurse_filters(filters[1], processing_func)]
        else:
            # ["And"/"Or", [child_filter, ...]]
            return [
                operator,
                [recurse_filters(child, processing_func) for child in filters[1]],
            ]

    # Simple filter: [attribute, operator, value] — pass whole filter to processing_func.
    # processing_func returns the filter unchanged if its conditions are not met.
    return processing_func(filters)


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
    search_results: Dict = field(default_factory=dict)


@dataclass
class ContentSearchExecutionContext:
    """Container used to inject objects into each agent run execution."""

    backend: TurbopufferBackend
    embedder: GoogleEmbedder
    edition_id: int
    search_results: Dict = field(default_factory=dict)
    frbr_fields: Dict = field(default_factory=dict)


@dataclass
class SnippetsExecutionContext:
    search_tool_call_id: str
    search_result: Dict


@dataclass
class Snippet:
    """Relevant snippet data as needed by frontend"""

    text: str
    item_id: Optional[
        int
    ]  # item id is because it links to a physical copy of a digital book
    chunk_score: Optional[float]
    start_page: Optional[int] = None
    end_page: Optional[int] = None


@dataclass(kw_only=True)
class BaseEditionResult:
    """Common base for all edition search result value objects."""

    edition_id: int
    chunk_hits: list
    snippets: List[Snippet] = field(default_factory=list)


@dataclass(kw_only=True)
class ContentSearchResult(BaseEditionResult):
    """In-book (single-edition) content search result."""


@dataclass(kw_only=True)
class CatalogSearchResult(BaseEditionResult):
    """Catalog (multi-edition) search result value object."""

    orm_work: Any
    orm_edition: Any
    agg_score: float


# TODO: make a name and args callback for any tool, and add a edition_id log message to search_book, add traceback log to this callback
# also time tool call construction latency
class LLMLoggingHooks(RunHooks):
    """Agent lifecycle hooks that log LLM call start/end with timing and response."""

    def __init__(self):
        self._llm_start_time: Optional[float] = None
        self._tool_start_time: Optional[float] = None

    async def on_llm_start(
        self,
        context: RunContextWrapper,
        agent: Agent,
        system_prompt: Optional[str],
        input_items: list,
    ) -> None:
        self._llm_start_time = time.perf_counter()

    async def on_llm_end(
        self,
        context: RunContextWrapper,
        agent: Agent,
        response: ModelResponse,
    ) -> None:
        elapsed = (
            time.perf_counter() - self._llm_start_time
            if self._llm_start_time is not None
            else None
        )
        self._llm_start_time = None
        elapsed_str = f"{elapsed:.3f}s" if elapsed is not None else "unknown"

        output_types = [o.type for o in response.output]
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

    Session = get_session()
    with Session() as session:
        result = session.execute(query, {"ids": list(ids)})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    logger.debug(
        f"Successfully mapped {len(df)} {source_col}s to {df[target_col].unique().size} {target_col}s"
    )

    # Create dict mapping: source id -> {target id -> value, ...}
    return df.set_index(source_col).to_dict(orient="index")


@timer(logger)
async def update_chat(conversation, conversation_type, edition_id=None) -> RunResult:
    """
    Send a message to the conversation and get the agent's response.

    The raw search results will be available in self.context.search_data
    for any post-processing or enrichment needed.

    Args:
        conversation: The list of openai Responses API items representing the conversation history.
        conversation_type: Either "contentSearch" or "catalogSearch" to pick the search mode.
        edition_id: Required when conversation_type is "contentSearch" so the agent knows which book to inspect.

    Returns:
        The agent's RunResult obj.
    """

    # TODO: when a user switches from catalog to content search, we should add \
    # an additional user message saying: "I am now switching to content search \
    # with in book XYZ" or "I am now switching back to catalog search"

    # TODO: figure out how to do thread safe  module level instantiations for \
    # some reused objs (backend, system prompts, async loop, etc...) (for sharing btw server \
    # request workers/threads)

    backend = TurbopufferBackend(index_name=INDEX_NAME, config=get_config())
    embedder = GoogleEmbedder(task_type="RETRIEVAL_QUERY")

    # NOTE: litellm has a bug converting `list | None = None` in agents sdk @functol_tool
    # param type annotations into gemini API compatible format

    # model = "litellm/gemini/gemini-3-flash-preview"
    model = OpenAIChatCompletionsModel(
        model="gemini-3-flash-preview",
        openai_client=AsyncOpenAI(
            api_key=require_env("GOOGLE_API_KEY"),
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        ),
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
        frbr_fields = format_frbr_fields(frbr_data[0].Work, frbr_data[0].Edition)

        exec_context = ContentSearchExecutionContext(
            backend=backend,
            embedder=embedder,
            edition_id=edition_id,
            frbr_fields=frbr_fields,
        )

        template = Template((PROMPTS_DIR / "system" / "1.jinja.md").read_text())
        system_prompt = template.render(
            conversation_type="contentSearch", frbr_fields=frbr_fields
        )
        tools = [search_book]

    # Search for books in catalog
    elif conversation_type == "catalogSearch":
        exec_context = CatalogSearchExecutionContext(backend=backend, embedder=embedder)
        template = Template((PROMPTS_DIR / "system" / "1.jinja.md").read_text())
        system_prompt = remove_markdown_comments(
            template.render(conversation_type="catalogSearch")
        )
        tools = [search_catalog]

    agent = Agent[type(exec_context)](
        name="Research Library Assistant",
        model=model,
        instructions=system_prompt,
        tools=tools,
    )

    run_result = await Runner.run(
        agent,
        conversation,
        context=exec_context,
        hooks=LLMLoggingHooks(),
        run_config=RunConfig(
            tracing_disabled=True,
            model_settings=ModelSettings(
                temperature=0.0,
                reasoning=Reasoning(effort="none"),
                # include_usage=True, # TODO: research if this returns loggable usage info
            ),
        ),
    )

    # Add relevant snippets, if search was executed
    # snippets updated in run_result in place
    await get_relevant_snippets(run_result)

    return run_result


def max_chunk_score(chunk_hits):
    return max([h["score"] for h in chunk_hits])


def min_chunk_score(chunk_hits):
    return min([h["score"] for h in chunk_hits])


def mean_chunk_score(chunk_hits):
    return np.mean([h["score"] for h in chunk_hits])


def results_to_chunk_hits(results: list[ScoredHit]) -> Iterator[dict[str, Any]]:
    """
    Yield chunk_hit's from search results. Adding item_id to each
    chunk_hit by mapping chunk record_id to item_id in DB.

    Args:
        results: List of (ChunkDocument, score) tuples
    """
    # chunk_hit = ChunkDocument + score (+ item_id)
    # NOTE: future: the item_id will be directly indexed in the chunk hit \
    # making this function unnecessary, ScoredHit can be used instead.

    missing_item_ids = []
    try:
        # Retrieve map of record_id->item_id from DB
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


CHUNK_SCORE_TYPE: Literal["higher-is-better", "lower-is-better"] = "higher-is-better"

if CHUNK_SCORE_TYPE == "higher-is-better":
    score_aggregator = max_chunk_score
    sort_direction = {"reverse": True}
    score_label = "MAX SCORE"
else:
    score_aggregator = min_chunk_score
    sort_direction = {"reverse": False}
    score_label = "MIN SCORE"


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
        query_vector = ctx.context.embedder.embed_one(ranking_query)

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

        # Group chunk hits by Edition (before adding FRBR data)
        # edition_hit = {"chunk_hits", "agg_score", "edition_id"}
        edition_hits = {}
        for chunk_hit in results_to_chunk_hits(results):
            edition_hits.setdefault(
                chunk_hit["edition_id"],
                {"edition_id": chunk_hit["edition_id"], "chunk_hits": []},
            )["chunk_hits"].append(chunk_hit)

        # Calculate aggregate edition score
        edition_hits = [
            {**eh, "agg_score": score_aggregator(eh["chunk_hits"])}
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
            edition_hits, key=lambda eh: eh["agg_score"], **sort_direction
        )

        # Limit results to the 10 top scoring editions
        logger.info(
            f"Limiting results to first {PAGE_SIZE} editions sorted by '{CHUNK_SCORE_TYPE}'"
        )
        edition_hits = edition_hits[:PAGE_SIZE]
        # TODO: handle paginating or providing more edition hits

        # Fetch FRBR data (from DB)
        edition_ids = [h["edition_id"] for h in edition_hits]
        logger.info(
            f"Fetching FRBR metadata for the following edition_ids: {edition_ids}"
        )
        with Timer(
            "get_frbr_data_by_edition",
            on_exit=lambda name, elapsed: logger.info(
                f"{name} took {elapsed:.3f}s for {len(edition_ids)} editions"
            ),
        ):
            frbr_data = get_frbr_data_by_edition(edition_ids)

        # Merge ES hit data and FRBR metadata (maintaining edition sort order)
        frbr_data = {row.Edition.id: row for row in frbr_data}
        edition_data = []  # list of EditionResult
        missing_data = []
        for edition_hit in edition_hits:
            # match DB orm work/edition to vector search edition hit
            row = frbr_data.get(edition_hit["edition_id"])
            if row is None:
                missing_data.append(edition_hit["edition_id"])
            else:
                edition_data.append(
                    CatalogSearchResult(
                        orm_work=row.Work,
                        orm_edition=row.Edition,
                        edition_id=edition_hit["edition_id"],
                        chunk_hits=edition_hit["chunk_hits"],
                        agg_score=edition_hit["agg_score"],
                    )
                )
        # ALT: if frbr_data was pre-sorted by edition_ids in the SQL call, we \
        # could zip frbr data and edition hits bc order would be the same
        if missing_data:
            logger.error(
                f"Vector search hits for the following edition_ids have no matching data in DB: {missing_data}"
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
        return format_search_results(
            [
                {
                    "frbr_fields": format_frbr_fields(e.orm_work, e.orm_edition),
                    "chunk_hits": e.chunk_hits,
                    "edition_id": e.edition_id,
                }
                for e in edition_data
            ],
            as_str=True,
        )

    except Exception as e:
        logger.exception(f"Error during {ctx.tool_name} tool execution.")
        raise e


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
            f"{ctx.tool_name} tool called with args: '{ctx.tool_arguments}', for edition_id = {ctx.context.edition_id}"
        )

        # Post-process filters through the pipeline
        filters = apply_filter_transforms(
            filters, apply_null_matching=filters_match_null
        )

        # Build filter to restrict search to single book
        book_filter = ["edition_id", "Eq", ctx.context.edition_id]

        # Combine with user filters if provided
        if filters is not None:
            combined_filters = ["And", [book_filter, filters]]
        else:
            combined_filters = book_filter

        # Embed the query for semantic search
        query_vector = ctx.context.embedder.embed_one(ranking_query)

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
                )
            ],
            "search_params": json.loads(ctx.tool_arguments),
        }

        # Format results for LLM
        return format_search_results(
            [
                {
                    "frbr_fields": ctx.context.frbr_fields,
                    "chunk_hits": chunk_hits,
                    "edition_id": ctx.context.edition_id,
                }
            ],
            as_str=True,
        )

    except Exception as e:
        logger.exception(f"Error during {ctx.tool_name} tool execution.")
        raise e


def exact_match(
    snippet_text: str, chunk_text: str
) -> tuple[Optional[str], Optional[float]]:
    """Match the generated snippet to chunk text.

    Returns (resolved_snippet, 1.0) if match is found, else (None, None).

    Matching Algo: the resolved snippet is the first whitespace-normalized exact
    string match of snippet_text in chunk_text.

    If snippet_text contains the elision token ``//...//``, it is split into lead
    and trail, which are matched against the chunk with a pattern requiring at
    least one character bridging them. Lead and trail must not be empty to match.
    """
    # NOTE: Since snippet text is indented in search tool response to LLM, we \
    # collapse all whitespace (including tabs) to single space.

    # elided snippet
    # TODO: guard against or handle multiple elisions
    if "//...//" in snippet_text:
        parts = snippet_text.split("//...//", 1)
        lead_tokens = parts[0].split()
        trail_tokens = parts[1].split()
        if not lead_tokens or not trail_tokens:
            return None, None
        lead_pat = r"\s+".join(re.escape(t) for t in lead_tokens)
        trail_pat = r"\s+".join(re.escape(t) for t in trail_tokens)
        pattern = lead_pat + r"\s+.+?\s+" + trail_pat

    # plain snippet
    else:
        tokens = snippet_text.split()
        if not tokens:
            return None, None
        pattern = r"\s+".join(re.escape(t) for t in tokens)

    m = re.search(pattern, chunk_text, re.DOTALL)
    return (m.group(0), 1.0) if m else (None, None)


# MAYBE: add a binary match/no-match version that doesn't expect a score
def find_snippet_in_chunks(
    snippet_text: str,
    chunk_hits: list,
    find_snippet_in_chunk: Callable[
        [str, str], tuple[Optional[str], Optional[float]]
    ] = exact_match,
) -> tuple[Optional[dict], Optional[str]]:
    """Search chunk_hits for the highest-scoring chunk matching snippet_text.

    Iterates all chunks, scores each via find_snippet_in_chunk, and returns the
    chunk with the highest score. In the case of ties the first chunk wins (strict >).

    Args:
        snippet_text: The snippet string to locate.
        chunk_hits: List of chunk hit dicts, each expected to have a "text" key.
        find_snippet_in_chunk: Callable(snippet_text, chunk_text) -> (resolved_snippet, score)
            where score is a float on match or None on no match. Defaults to exact_match
            which returns score=1.0 on match.

    Returns:
        (matched_chunk_hit, resolved_snippet) or (None, None) if no chunk matched.
    """
    best_chunk_hit = None
    best_resolved_snippet = None
    best_score = -1.0
    for chunk_hit in chunk_hits:
        resolved_snippet, score = find_snippet_in_chunk(
            snippet_text, chunk_hit.get("text", "")
        )
        if score is not None and score > best_score:
            best_score = score
            best_chunk_hit = chunk_hit
            best_resolved_snippet = resolved_snippet
    if best_chunk_hit is not None:
        logger.debug(
            f"find_snippet_in_chunks best score: {best_score:.2f} for snippet '{snippet_text[:60]}'"
        )
    return best_chunk_hit, best_resolved_snippet


def text_processor(s):
    # TODO: add custom processor that collapses "-\n"->""
    # MAYBE: ascii folding
    # set all non word, digit, whitespace to empty str
    s = re.sub(r"[^\w\d\s]", "", s)
    # collapse one or more whitespace char to single space
    s = re.sub(r"\s+", " ", s)
    s = s.lower()
    return s


def fuzzy_match(snippet, chunk):
    """
    Edit distance of best match substring

    Start and end in return value are in post-processed text strings
    """

    r = fuzz.partial_ratio_alignment(snippet, chunk, processor=text_processor)
    return {"score": r.score, "start": r.dest_start, "end": r.dest_end}


def fuzzy_match_elipsis(snippet_text, chunk_text, threshold=88):
    """Match the generated snippet to chunk text.

    Splits snippet by `...` and match each part in sequence.

    Returns (snippet_text, min_part_score) if match is found, else (snippet_text, None).

    Part Match Algo: remove alpha num chars, collapse+trim whitespace, lowercase
    then check if best chunk substring edit distance is above threshold.
    """
    # MAYBE: text_process all at start
    parts = [p for p in snippet_text.strip("...").split("...") if text_processor(p)]
    if len(parts) == 0:
        return snippet_text, None
    results = [fuzzy_match(part, chunk_text) for part in parts]
    scores = [r["score"] for r in results]
    all_match = all(s >= threshold for s in scores)
    # NOTE: if there are multiple matches some sequential and some not, it is
    # possible fuzz would return a match that is not sequential even if there is
    # a sequential match.
    # NOTE: partial_ratio dest_end may extend beyond final matching block (bc
    # of matching block selection algo)
    all_sequential = (
        all(
            results[i]["end"] <= results[i + 1]["start"]
            for i in range(len(results) - 1)
        )
        if all_match
        else False
    )

    if all_match and all_sequential:
        return snippet_text, min(scores)
    else:
        return snippet_text, None


def _format_no_match_diff(snippet_text: str, chunk_hits: list) -> str:
    """Return a human-readable word-level diff between the submitted snippet
    and the closest matching region found across all chunk hits.

    Uses SequenceMatcher to locate the best-aligned window inside each chunk,
    then formats a word-level ndiff so it's easy to spot where the LLM
    misquoted the source text.
    """
    snippet_norm = " ".join(snippet_text.split())
    snippet_words = snippet_norm.split()
    if not snippet_words:
        return "  (empty snippet)"

    best_ratio = -1.0
    best_chunk_window_words: list[str] = []

    for chunk_hit in chunk_hits:
        chunk_norm = " ".join(chunk_hit.get("text", "").split())
        chunk_words = chunk_norm.split()
        if not chunk_words:
            continue

        # work token diff
        sm = difflib.SequenceMatcher(None, snippet_words, chunk_words, autojunk=False)
        ratio = sm.ratio()

        # Locate the region of the chunk best aligned with the snippet.
        # Pad by a few words on each side so trailing/leading insertions are visible.
        real_blocks = [b for b in sm.get_matching_blocks() if b.size > 0]
        if not real_blocks:
            continue
        WINDOW_PAD = 5
        chunk_start = max(0, real_blocks[0].b - WINDOW_PAD)
        chunk_end = min(
            len(chunk_words), real_blocks[-1].b + real_blocks[-1].size + WINDOW_PAD
        )
        window_words = chunk_words[chunk_start:chunk_end]

        if ratio > best_ratio:
            best_ratio = ratio
            best_chunk_window_words = window_words

    # No matching token blocks in any chunk
    if not best_chunk_window_words:
        return f"  snippet (normalized): {repr(snippet_norm[:300])}"

    diff_lines = list(difflib.ndiff(snippet_words, best_chunk_window_words))

    lines = [
        f"  best chunk similarity: {best_ratio:.1%}",
        "  word diff - each line is a word token  (- submitted by LLM, + actual text in chunk):",
    ]
    lines.extend(f"    {line}" for line in diff_lines)
    return "\n".join(lines)


class RejectionCode(str, Enum):
    SNIPPET_COUNT = "SNIPPET_COUNT"
    NO_MATCH = "NO_MATCH"
    WORD_LIMIT = "WORD_LIMIT"


@dataclass
class Rejection:
    edition_id: int
    snippet: Optional[str]
    code: RejectionCode
    data: dict = field(default_factory=dict)


def build_rejection_message(rejected: List[Rejection]) -> str:
    """Build the full rejection response string from a list of Rejection objects."""

    def _snippet_preview(r: Rejection) -> str:
        if not r.snippet:
            return ""
        s = r.snippet[:60] + ("..." if len(r.snippet) > 60 else "")
        return f"snippet {repr(s)}: "

    _MESSAGES: dict[RejectionCode, Callable[[Rejection], str]] = {
        RejectionCode.SNIPPET_COUNT: lambda r: (
            "0 snippets submitted; please submit at least one snippet."
        ),
        RejectionCode.NO_MATCH: lambda r: (
            """Submitted snippet text was not able to be matched to any chunk. 
Ensure the text is copied character by character from the chunk"""
            + (
                f"""Here is a word-level diff between your submitted snippet and the closest matching chunk. Use this diff to identify how you need to correct your snippet.
{r.data["diff"]}"""
                if r.data.get("diff")
                else ""
            )
            # ", if using elision, lead/trail are copied verbatim and that non-empty text appears on both sides of '//...//`'.
        ),
        RejectionCode.WORD_LIMIT: lambda r: (
            f"Resolved snippet has {r.data['word_count']} words, exceeding the "
            f"150-word hard limit (100-word target + 50-word grace margin). "
            f"Please shorten or use tighter elision."
        ),
    }

    blocks = [
        f"""{len(rejected)} snippet(s) did not pass validation.
Follow the instructions below for guidance on what to resubmit and how to correct your submission."""
    ]
    for r in rejected:
        blocks.append(
            f"  REJECTED SNIPPET:{_snippet_preview(r)}\n{indent(_MESSAGES[r.code](r), '  ')}"
        )
    blocks.append(
        "In your next response, include ONLY the corrected snippets listed above."
    )
    return "\n\n".join(blocks)


class _SnippetSelectionResponse(BaseModel):
    snippets: List[str]


def validate_edition_snippets(
    edition_id: int,
    snippet_list: List[str],
    chunk_hits: list,
    find_snippet_in_chunk: Callable[
        [str, str], tuple[Optional[str], Optional[float]]
    ] = exact_match,
) -> Tuple[List[Rejection], List[Snippet]]:
    """Validate a list of submitted snippets against the chunk hits for one edition.

    Pure function — no side effects. Returns (rejections, validated_snippets) where
    validated_snippets are fully-formed Snippet objects ready to extend an entry's .snippets.
    This is the natural unit for future parallel-edition processing.

    Args:
        edition_id: The edition being validated (used only for rejection messages).
        snippet_list: Submitted snippet strings.
        chunk_hits: The chunk hits stored for this edition.
        find_snippet_in_chunk: Callable(snippet_text, chunk_text) -> (resolved_snippet, score)
            passed directly to find_snippet_in_chunks. Defaults to exact_match.

    Returns:
        Tuple of (list of Rejection objects, list of valid Snippet objects).
    """
    rejections: List[Rejection] = []
    validated: List[dict] = []

    # --- Guard: reject if LLM submitted zero snippets ---
    # NOTE: if we want to make this a substantive minimum snippets per edition \
    # validation, we need to either pull the check into the caller or pass the \
    # entry.snippets state into the function to insure it is a cumulative minimum \
    # per edition over multiple runs.
    if not snippet_list:
        logger.debug(f"Rejecting edition {edition_id}: 0 snippets submitted.")
        return (
            [Rejection(edition_id, None, RejectionCode.SNIPPET_COUNT, {"count": 0})],
            [],
        )

    for snippet_text in snippet_list:
        # --- Step 1: Validate snippet matches edition chunk ---
        matched_chunk, resolved_snippet = find_snippet_in_chunks(
            snippet_text, chunk_hits, find_snippet_in_chunk
        )

        if matched_chunk is None:
            diff_log = _format_no_match_diff(snippet_text, chunk_hits)
            logger.debug(
                f"Rejecting snippet for edition {edition_id}: not found in any chunk.\n"
                f"  submitted snippet: {repr(snippet_text[:80])}\n"
                f"{diff_log}"
            )
            rejections.append(
                Rejection(
                    edition_id, snippet_text, RejectionCode.NO_MATCH, {"diff": diff_log}
                )
            )
            continue

        # --- Step 2: Validate word count on the resolved snippet ---
        word_count = len(resolved_snippet.split())
        if word_count > 150:
            logger.debug(
                f"Rejecting snippet for edition {edition_id}: resolved snippet "
                f"has {word_count} words, exceeding 150-word hard limit."
            )
            rejections.append(
                Rejection(
                    edition_id,
                    snippet_text,
                    RejectionCode.WORD_LIMIT,
                    {"word_count": word_count},
                )
            )
            continue

        # All validations passed — build the storable snippet
        validated.append(
            Snippet(
                text=resolved_snippet,
                start_page=matched_chunk.get("start_page")
                or matched_chunk.get("chunk_start_page"),
                end_page=matched_chunk.get("end_page")
                or matched_chunk.get("chunk_end_page"),
                item_id=matched_chunk.get("item_id"),
                chunk_score=matched_chunk.get("score")
                or matched_chunk.get("meta", {}).get("score"),
            )
        )

    return rejections, validated


# TODO: insert search result extracted here where appropriate
def _build_conversation_text(run_result: RunResult) -> str:
    """Extract user and assistant text messages from run_result as a formatted conversation string."""

    def _get_clean_text(message):
        """Standardize text extraction from dict serialization of type=message
        OpenAI Responses API items
        """
        content = message.get("content", "")
        if isinstance(content, list):
            return "".join(
                part.get("text", "")
                for part in content
                if part.get("type") == "output_text"
            )
        return str(content)

    parts = []
    for msg in run_result.to_input_list():
        role = msg.get("role", "")
        if role not in ("user", "assistant"):
            continue
        text = _get_clean_text(msg).strip()
        if text:
            label = "User" if role == "user" else "Assistant"
            parts.append(f"{label}: {text}")
    return "\n\n".join(parts)


def _extract_search_result_text(
    run_result: RunResult, search_tool_call_id: str
) -> Optional[str]:
    """Find the string output of the search tool call in run_result.new_items.

    Returns None if the matching ToolCallOutputItem is not found.
    """
    for item in run_result.new_items:
        if not isinstance(item, ToolCallOutputItem):
            continue
        raw = item.raw_item
        call_id = (
            raw.get("call_id") if hasattr(raw, "get") else getattr(raw, "call_id", None)
        )
        if call_id == search_tool_call_id:
            return str(item.output)
    return None


def _apply_naive_snippets(entry: "BaseEditionResult") -> None:
    """Append a naive truncated snippet for every chunk_hit in entry.

    Does not skip entries that already have snippets.
    """
    for chunk_hit in entry.chunk_hits:
        entry.snippets.append(
            Snippet(
                text=shorten(chunk_hit["text"]),
                start_page=chunk_hit.get("start_page")
                or chunk_hit.get("chunk_start_page"),
                end_page=chunk_hit.get("end_page") or chunk_hit.get("chunk_end_page"),
                item_id=chunk_hit.get("item_id"),
                chunk_score=chunk_hit.get("score")
                or chunk_hit.get("meta", {}).get("score"),
            )
        )


def get_relevant_snippets_naive(run_result: RunResult) -> Optional[bool]:
    """Naively populate snippets for all editions in the last search result using
    simple text truncation — no AI selection.

    Same input/output/side-effect shape as get_relevant_snippets: returns None if
    no search results, True on success, and updates search_results in place.
    Available as a lightweight fallback or for offline use.
    """
    search_results = run_result.context_wrapper.context.search_results
    if not search_results:
        return None

    _, search_result = list(search_results.items())[-1]
    edition_data = search_result.get("edition_data", [])

    for entry in edition_data:
        if entry.snippets:
            continue
        _apply_naive_snippets(entry)

    return True


_SNIPPET_AGENT_MAX_TURNS = 1
_SNIPPET_AGENT_MAX_CONCURRENT = 20


class EditionSnippetLoop:
    """Self-validating LLM snippet selection loop for a single edition.

    Instantiate with the loop configuration, then call run() to execute.
    Agent instruction are passed in the form of messages = (system + initial user turn).

    """

    def __init__(
        self,
        client: AsyncOpenAI,
        model_name: str,
        messages: list,
        entry: BaseEditionResult,
    ) -> None:
        self.client = client
        self.model_name = model_name
        self.messages = messages
        self.entry = entry
        self.model_config: dict = {"temperature": 0.0, "reasoning_effort": "none"}
        self.last_response = None
        self.n_turns: int = 0
        self.max_turns_exceeded: bool = False

    async def run(self) -> None:
        """Run the snippet selection loop.

        Modifies entry.snippets in place on success.
        Sets max_turns_exceeded=True if all turns are exhausted without a fully
        valid submission. last_response and n_turns are updated incrementally
        and can be inspected if this method raises.
        """
        for turn in range(_SNIPPET_AGENT_MAX_TURNS):
            # Get LLM response
            t0 = time.perf_counter()  # TODO: use Timer()
            # MAYBE: Add edition_id or entry as Loop context
            response = await self.client.chat.completions.parse(
                model=self.model_name,
                messages=self.messages,
                response_format=_SnippetSelectionResponse,
                # equivalent to run_config.model_settings
                **self.model_config,
            )
            self.last_response = response
            self.n_turns = turn + 1
            elapsed = time.perf_counter() - t0
            usage = response.usage
            logger.info(
                f"Snippet agent edition {self.entry.edition_id}"
                f" | turn {self.n_turns}/{_SNIPPET_AGENT_MAX_TURNS}"
                f" | elapsed: {elapsed:.3f}s"
                f" | finish_reason: {response.choices[0].finish_reason}"
                f" | refusal: {response.choices[0].message.refusal}"
                + (
                    f" | tokens: input={usage.prompt_tokens} output={usage.completion_tokens}"
                    if usage
                    else ""
                )
            )

            # Guard: non-stop finish reasons (e.g. RECITATION content filter) are unrecoverable
            choice = response.choices[0]
            if choice.finish_reason != "stop":
                raise RuntimeError(
                    f"Snippet agent edition {self.entry.edition_id}: non-stop finish_reason"
                    f" '{choice.finish_reason}' (refusal: {choice.message.refusal})"
                )

            # Extract structured output
            parsed = choice.message.parsed
            if parsed is None:
                logger.debug(
                    f"Snippet agent edition {self.entry.edition_id}: structured output is `None` on turn {self.n_turns}; treating as []."
                )
                snippet_list = []
            else:
                snippet_list = parsed.snippets

            logger.info(
                f"Snippet agent edition {self.entry.edition_id}: {len(snippet_list)} snippet(s) submitted for validation,"
                f" {len(self.entry.chunk_hits)} chunk(s) available."
            )

            # Validate selected snippets
            rejections, validated = validate_edition_snippets(
                self.entry.edition_id,
                snippet_list,
                self.entry.chunk_hits,
                find_snippet_in_chunk=fuzzy_match_elipsis,
            )
            # Save valid snippets
            # MAYBE: consider not setting result in place
            self.entry.snippets.extend(validated)
            if validated:
                for s in validated:
                    snippet_lead = repr(s.text[:50]).strip("'\"")
                    logger.debug(
                        f"Snippet agent edition {self.entry.edition_id}: saved snippet '{snippet_lead}...'"
                    )
                logger.info(
                    f"Snippet agent edition {self.entry.edition_id}: {len(validated)} snippet(s) saved (turn {self.n_turns})."
                )

            if not rejections:
                return

            logger.info(
                f"Snippet agent edition {self.entry.edition_id}: {len(rejections)} rejection(s) on turn {self.n_turns}."
            )

            # Request corrections
            # MAYBE: update system-prompt/first-message rather than extending convo
            rejection_message = build_rejection_message(rejections)
            assistant_content = choice.message.content or (
                json.dumps(parsed.model_dump()) if parsed is not None else ""
            )
            self.messages.append({"role": "assistant", "content": assistant_content})
            self.messages.append({"role": "user", "content": rejection_message})

        logger.error(
            f"Snippet agent edition {self.entry.edition_id} failed to correctly submit all originally submitted snippets after {_SNIPPET_AGENT_MAX_TURNS} turns."
        )
        self.max_turns_exceeded = True


# TODO: fall back to naive snippets if edition has less than 1 (2?) snippets (even when an error occurs in get_relevant_snippets)
@timer(logger)
async def get_relevant_snippets(
    run_result: RunResult,
    fallback_naive: bool = True,
) -> Optional[List[EditionSnippetLoop]]:
    """Run a snippet agent to select and store relevant text snippets for the last
    search result in run_result.

    Runs all editions concurrently, with one self-validating LLM snippet
    selection loop per edition.
    Returns None if no search results, [] if all editions already had snippets,
    or a list of EditionSnippetLoop (one per edition processed); inspect loop
    attributes for per-edition state after the call.
    Updates run_result.context_wrapper.context.search_results in place.

    Args:
        fallback_naive: If True (default), editions with no AI-selected snippets
            fall back to naive truncated snippets. Set False to disable the
            fallback.
    """
    search_results = run_result.context_wrapper.context.search_results
    if not search_results:
        logger.warning(
            "get_relevant_snippets: no search results found in agent run_result."
        )  # TODO: deduplicate, also  logged in format_search_results
        return None

    # Select last search result
    _, search_result = list(search_results.items())[-1]

    # Skip editions that already have snippets
    # TODO: figure out how to add preexisting snippet editions to logging and aggregation at end, maybe no early return
    edition_data = []
    for e in search_result.get("edition_data", []):
        if e.snippets:
            logger.info(
                f"get_relevant_snippets: edition {e.edition_id} already has {len(e.snippets)} snippet(s); skipping."
            )
        else:
            edition_data.append(e)
    if not edition_data:
        logger.info("get_relevant_snippets: all editions already have snippets.")
        return []

    # Extract common variables for all editions
    conversation_text = _build_conversation_text(run_result)
    client: AsyncOpenAI = run_result.last_agent.model._client
    model_name: str = run_result.last_agent.model.model
    # model_name = "gemini-3.1-flash-lite-preview"
    # model_name = 'gemini-2.5-flash-lite'
    prompt_template = Template(
        (PROMPTS_DIR / "snippet_agent" / "v7.jinja.md").read_text()
    )

    # Make selection task for each edition
    loops: List[EditionSnippetLoop] = []
    tasks = []
    for entry in edition_data:
        # Format search result chunk text
        frbr_fields = (
            format_frbr_fields(entry.orm_work, entry.orm_edition)
            if isinstance(entry, CatalogSearchResult)
            else run_result.context_wrapper.context.frbr_fields
        )
        edition_chunk_text = format_search_results(
            [
                {
                    "frbr_fields": frbr_fields,
                    "chunk_hits": entry.chunk_hits,
                    "edition_id": entry.edition_id,
                }
            ],
            as_str=True,
        )

        snippet_agent_prompt = remove_markdown_comments(
            prompt_template.render(
                system_prompt=run_result.last_agent.instructions,
                search_tool_name=run_result.last_agent.tools[0].name,
                search_tool_description=run_result.last_agent.tools[0].description,
                conversation_text=conversation_text,
                search_result_text=edition_chunk_text,
            )
        )
        # TODO: test that the main agent has only 1 tool (this is assumed in writing
        # the relevant snippet prompt)
        messages = [
            {"role": "system", "content": snippet_agent_prompt},
            {"role": "user", "content": "Begin snippet selection."},
        ]
        loop = EditionSnippetLoop(client, model_name, messages, entry)
        loops.append(loop)
        tasks.append(loop.run())

    semaphore = asyncio.Semaphore(_SNIPPET_AGENT_MAX_CONCURRENT)

    async def _gated(coro):
        """max concurrency wrapper"""
        async with semaphore:
            return await coro

    logger.info(
        f"get_relevant_snippets: running snippet agent for {len(tasks)} edition(s) concurrently"
        f" (max {_SNIPPET_AGENT_MAX_CONCURRENT} at a time)."
    )
    # MAYBE: handle tokens per minute rate limit errors explicitly with exponential backoff?
    results = await asyncio.gather(*(_gated(t) for t in tasks), return_exceptions=True)

    # Log results + Apply fallback snippets
    n_errored = 0  # raised an exception
    n_incomplete = 0  # hit max turns with at least one invalid submission
    n_no_snippets = 0  # zero AI-selected snippets (fell back to naive)
    total_snippets = 0
    for loop, entry, result in zip(loops, edition_data, results):
        if isinstance(result, Exception):
            logger.exception(
                f"Snippet agent edition {entry.edition_id}: raised an exception"
                f" after {loop.n_turns} turn(s).",
                exc_info=result,
            )
            n_errored += 1
        elif loop.max_turns_exceeded:
            n_incomplete += 1

        # Fallback to naive snippets if selection failed
        selected_count = len(entry.snippets)
        if not entry.snippets:
            n_no_snippets += 1
            if fallback_naive:
                logger.warning(
                    f"Snippet agent edition {entry.edition_id}: no snippets saved; "
                    f"falling back to naive snippets."
                )
                _apply_naive_snippets(entry)
            else:
                logger.warning(
                    f"Snippet agent edition {entry.edition_id}: no snippets saved "
                    f"(naive fallback disabled)."
                )
        else:
            logger.info(
                f"Snippet agent edition {entry.edition_id}: {selected_count} AI-selected snippet(s) saved"
                f" in {loop.n_turns} turn(s)."
            )
        total_snippets += selected_count

    logger.info(
        f"get_relevant_snippets: Edition summary: {len(edition_data)} processed"
        f" | {n_no_snippets} with no snippets saved"
        f" | {n_incomplete} invalid snippets after max turns"
        f" | {n_errored} errored"
    )
    logger.info(f"get_relevant_snippets: {total_snippets} total AI-selected snippet(s)")

    return loops


def format_frbr_fields(orm_work, orm_edition):
    """
    Format ORM work and edition attributes for printing.
    """
    # Format work metadata
    title = orm_work.title or "(No Title)"

    authors = orm_work.authors or []
    author_names = (
        ", ".join([a.get("name", "") for a in authors if isinstance(a, dict)])
        if authors
        else "(No Authors)"
    )

    subjects = orm_work.subjects or []
    subject_list = (
        ", ".join([s.get("heading", "") for s in subjects if isinstance(s, dict)])
        if subjects
        else "(None)"
    )

    # Format edition metadata
    pub_date = (
        str(orm_edition.publication_date)
        if orm_edition.publication_date
        else "(No Date)"
    )

    publishers = orm_edition.publishers or []
    publisher_names = (
        ", ".join([p.get("name", "") for p in publishers if isinstance(p, dict)])
        if publishers
        else "(No Publisher)"
    )

    # Format language metadata
    languages = orm_edition.languages or []
    language_list = (
        ", ".join(
            [
                lang.get("language", "") if isinstance(lang, dict) else str(lang)
                for lang in languages
            ]
        )
        if languages
        else "(No Language)"
    )

    return {
        "title": title,
        "author_names": author_names,
        "subject_list": subject_list,
        "pub_date": pub_date,
        "publisher_names": publisher_names,
        "language_list": language_list,
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


# MAYBE: remove book level info from search response  for contentSearch to save tokens.
def format_search_results(
    edition_data, search_tool_call_id=None, query=None, as_str=False
):
    """
    Print or return a formatted str containing an ordered list of editions and their
    associated text excerpts. For each edition, metadata (title, authors, subjects,
    publication date) and chunk text excerpts with page numbers are displayed.
    Editions are ordered by the input list.

    Args:
        edition_data: List of dicts with keys 'frbr_fields', 'chunk_hits', 'edition_id'
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
        lines = display_book(
            lines, entry["frbr_fields"], entry["chunk_hits"], entry["edition_id"]
        )

    lines.append("\n</search_results>")

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


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
