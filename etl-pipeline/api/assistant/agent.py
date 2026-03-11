from dataclasses import dataclass, field, asdict
from datetime import datetime
from importlib.resources import read_text
import json
from pathlib import Path
import re
import traceback
import asyncio
from typing import Callable, Dict, Any, Optional, Union, List
from typing_extensions import TypedDict
from enum import Enum
import uuid
from textwrap import indent
import sys
import os
import asyncio
import numpy as np
import pandas as pd

from agents import (
    Agent,
    OpenAIChatCompletionsModel,
    Runner,
    RunConfig,
    function_tool,
    RunContextWrapper,
    SQLiteSession,
    ModelSettings,
    RunResult,
)
from agents.tool_context import ToolContext
from agents.extensions.memory import SQLAlchemySession
from openai import AsyncOpenAI
from openai.types.shared import Reasoning
from sqlalchemy import text
from jinja2 import Template


# from sqlalchemy.ext.asyncio import create_async_engine

# api code
from ..utils import APIUtils, hit_to_dict, remove_markdown_comments
from ..db import get_frbr_data_by_edition, get_session

# shared code
from vector_indexing.components.embedders.google import GoogleEmbedder
from vector_indexing.components.backends.turbopuffer import TurbopufferBackend
from vector_indexing.core.config import get_config
from vector_indexing.core.utils import Timer
from logger import create_log
from utils.common import wrap, require_env


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

SELECT_RELEVANT_SNIPPETS_DOC = (
    # PROMPTS_DIR / "tools" / "select_relevant_snippets.txt"
    PROMPTS_DIR / "tools" / "select_relevant_snippets_no_elision.txt"
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


def recurse_filters(filters: Any, processing_func: callable) -> Any:
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
    item_id: int
    search_results: Dict = field(default_factory=dict)
    frbr_fields: Dict = field(default_factory=dict)


@dataclass
class SnippetsExecutionContext:
    search_tool_call_id: str
    search_result: Dict


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


def update_chat(conversation, conversation_type, edition_id=None) -> RunResult:
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
    embedder = GoogleEmbedder()

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
        # TEMP: convert edition_id to record_id to filter ES search
        mapped_ids = map_editions_and_records(edition_ids=[edition_id])[edition_id]
        record_id = mapped_ids["record_id"]
        item_id = mapped_ids["item_id"]  # BUG: this item_id may not be correct \
        # for the returned chunks in the case of multiple items per edition, in \
        # that case it was arbitrarily selected by map_editions_and_records().

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

        # NOTE: intentionally passing record_id as edition_id to make future state a smaller refactor
        # NOTE: future item_id will be extracted directly from the chunk hit, not passed from the mapper as here
        exec_context = ContentSearchExecutionContext(
            backend=backend,
            embedder=embedder,
            edition_id=record_id,
            item_id=item_id,
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
        # model_settings=ModelSettings(
        #     # include_usage=True,  # only for chatcompletions based agents/models # requires openai model?
        #     # reasoning=Reasoning(effort="low"),  # converted to chat completions API  reasoning_effort= which  is consistently supported in litellm
        # ),
        instructions=system_prompt,
        tools=tools,
    )

    run_result = Runner.run_sync(
        agent,
        conversation,
        context=exec_context,
        run_config=RunConfig(
            tracing_disabled=True, model_settings=ModelSettings(temperature=0.0)
        ),
    )

    # TODO: test that the main agent has only 1 tool (this is assumed in writing the relevant snippet prompt)
    # Add relevant snippets if search was executed
    # r = get_relevant_snippets(run_result, system_prompt)
    r = None

    return run_result, r


def max_chunk_score(chunk_hits):
    return max([h["score"] for h in chunk_hits])


def mean_chunk_score(chunk_hits):
    return np.mean([h["score"] for h in chunk_hits])


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

        # Execute vector search via TurbopufferBackend
        # take top 100 chunks and group by edition (then take top 10 editions)
        results = ctx.context.backend.query(
            rank_by=("vector", "ANN", query_vector),
            top_k=100,
            filters=filters,
        )
        logger.info(f"Retrieved {len(results)} chunk hits from vector backend")

        if not len(results):
            return "No results found for your query."

        # MAYBE: turn the below into 2 functions: group_by_edition_and_sort() and enrich_edition_hits() (with limit to top 10 in between)

        # Group ES Chunk hits by Edition (before adding FRBR data)
        record_ids = set(cd.book_id for cd, _ in results)
        mapper = map_editions_and_records(record_ids=record_ids)
        edition_hits = {}
        missing_edition_ids = []
        # Results from TurbopufferBackend are list of (ChunkDocument, distance) tuples
        for chunk_doc, distance in results:
            chunk_hit = chunk_doc.to_dict()
            chunk_hit["score"] = distance if distance is not None else 0.0
            if not chunk_hit.get("book_id"):
                logger.error(
                    f"Chunk missing book_id: id={chunk_hit.get('doc_id')}, keys={chunk_hit.keys()}"
                )
                continue
            # book_id was incorrectly indexed as a str for a while
            chunk_hit["book_id"] = int(chunk_hit["book_id"])

            # convert record_ids to edition_ids (for fetching FRBR data)
            # TODO: FUTURE: index books with book_id=edition_id so we don't have to convert
            frbr_ids = mapper.get(chunk_hit["book_id"])
            if frbr_ids is None:
                missing_edition_ids.append(chunk_hit["book_id"])
                continue
            chunk_hit.update(frbr_ids)
            if frbr_ids["edition_id"] not in edition_hits:
                edition_hits[frbr_ids["edition_id"]] = {
                    "edition_id": frbr_ids["edition_id"],
                    "chunk_hits": [chunk_hit],
                }
            else:
                edition_hits[frbr_ids["edition_id"]]["chunk_hits"].append(chunk_hit)
        # set aggregate edition score
        edition_hits = [
            {**eh, "agg_score": max_chunk_score(eh["chunk_hits"])}
            for eh in edition_hits.values()
        ]
        logger.info(
            f"Aggregated {len(edition_hits)} editions from {len(results)} chunk hits"
        )
        if missing_edition_ids:
            logger.error(
                f"These {len(set(missing_edition_ids))} record_ids failed to map to an edition: {set(missing_edition_ids)}"
            )
            # this case of missing record_ids is also logged in map_editions_and_records()

        # TODO: if fewer than PAGE_SIZE editions, re-query more chunks until PAGE_SIZE editions are retrieved

        # Sort editions (by aggregate chunk score)
        edition_hits = sorted(edition_hits, key=lambda eh: eh["agg_score"])

        # Limit results to the 10 top scoring editions
        logger.info(f"Limiting results to top {PAGE_SIZE} editions")
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
        edition_data = []  # dict with keys: orm_work, orm_edition, edition_hit
        missing_data = []
        for edition_hit in edition_hits:
            # match DB orm work/edition to vector search edition hit
            row = frbr_data.get(edition_hit["edition_id"])
            if row is None:
                missing_data.append(edition_hit["edition_id"])
            else:
                edition_data.append(
                    {
                        "orm_work": row.Work,
                        "orm_edition": row.Edition,
                        "edition_hit": edition_hit,
                    }
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
        return verbose_display_editions(edition_data, as_str=True)

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
            f"{ctx.tool_name} tool called with args: '{ctx.tool_arguments}', for edition_id (record_id) = {ctx.context.edition_id}"
        )

        # Post-process filters through the pipeline
        filters = apply_filter_transforms(
            filters, apply_null_matching=filters_match_null
        )

        # Build filter to restrict search to single book
        # NOTE: book_id was (incorrectly) indexed as str initially
        # TODO: current book_id=record_id, future book_id=edition_id (record_id currently passed to context under name edition_id)
        book_filter = ["book_id", "Eq", str(ctx.context.edition_id)]

        # Combine with user filters if provided
        if filters is not None:
            combined_filters = ["And", [book_filter, filters]]
        else:
            combined_filters = book_filter

        # Embed the query for semantic search
        query_vector = ctx.context.embedder.embed_one(ranking_query)

        # Execute vector search via TurbopufferBackend
        results = ctx.context.backend.query(
            rank_by=("vector", "ANN", query_vector),
            top_k=10,
            filters=combined_filters,
        )
        logger.info(f"Retrieved {len(results)} chunk hits from vector backend for book")

        if not len(results):
            return "No results found for your query in this book."

        # Convert chunk hits to dict format
        chunk_hits = []
        for chunk_doc, distance in results:
            chunk_hit = chunk_doc.to_dict()
            chunk_hit["item_id"] = (
                ctx.context.item_id
            )  # NOTE: future: the item_id will be directly indexed in the chunk hit.
            chunk_hit["score"] = distance if distance is not None else 0.0
            chunk_hits.append(chunk_hit)

        # Store search results for later reference
        ctx.context.search_results[ctx.tool_call_id] = {
            "tool_name": ctx.tool_name,
            "chunk_hits": chunk_hits,
            # "work": orm_work,
            # "edition": orm_edition,
            "search_params": json.loads(ctx.tool_arguments),
        }

        # Format results for LLM
        return verbose_display_chunks(
            chunk_hits, as_str=True, frbr_fields=ctx.context.frbr_fields
        )

    except Exception as e:
        logger.exception(f"Error during {ctx.tool_name} tool execution.")
        raise e


def find_snippet_in_chunk(snippet_text: str, chunk_text: str) -> Optional[str]:
    """Return the first whitespace-normalized match of snippet_text in chunk_text,
    or None if not found.

    If snippet_text contains the elision token ``//...//``, it is split into lead
    and trail. Both are whitespace-normalized into a regex pattern; a lazy ``.+?``
    (requiring at least one character) bridges them so any intervening text is
    captured. Non-empty lead and trail are required — if either is empty after
    splitting, None is returned.

    Plain snippets are also matched with whitespace normalization so that
    differences in spacing or line breaks in the source do not prevent a match.
    """
    # NOTE: Since snippet text is indented in search tool response to LLM, we \
    # collapse all whitespace (including tabs) to single space.

    # elided snippet
    if "//...//" in snippet_text:
        parts = snippet_text.split("//...//", 1)
        lead_tokens = parts[0].split()
        trail_tokens = parts[1].split()
        if not lead_tokens or not trail_tokens:
            return None
        lead_pat = r"\s+".join(re.escape(t) for t in lead_tokens)
        trail_pat = r"\s+".join(re.escape(t) for t in trail_tokens)
        pattern = lead_pat + r"\s+.+?\s+" + trail_pat

    # plain snippet
    else:
        tokens = snippet_text.split()
        if not tokens:
            return None
        pattern = r"\s+".join(re.escape(t) for t in tokens)

    m = re.search(pattern, chunk_text, re.DOTALL)
    return m.group(0) if m else None


class RejectionCode(str, Enum):
    MISSING_EDITION = "MISSING_EDITION"
    SNIPPET_COUNT = "SNIPPET_COUNT"
    NO_MATCH = "NO_MATCH"
    WORD_LIMIT = "WORD_LIMIT"
    ITEM_ID_MISMATCH = "ITEM_ID_MISMATCH"
    HALLUCINATED_EDITION = "HALLUCINATED_EDITION"


@dataclass
class Rejection:
    edition_id: int
    snippet_obj: Optional[dict]
    code: RejectionCode
    data: dict = field(default_factory=dict)


def build_rejection_message(rejected: List[Rejection], search_tool_call_id: str) -> str:
    """Build the full rejection response string from a list of Rejection objects."""

    def _snippet_preview(r: Rejection) -> str:
        if r.snippet_obj and r.snippet_obj.get("snippet"):
            raw = r.snippet_obj["snippet"]
            preview = raw[:60] + ("..." if len(raw) > 60 else "")
            return f" | snippet: {repr(preview)}"
        return ""

    _MESSAGES: dict[RejectionCode, Callable[[Rejection], str]] = {
        RejectionCode.MISSING_EDITION: lambda r: (
            f"Edition {r.edition_id} has no snippets saved and was not included "
            f"in this submission. Every edition in the search result requires "
            f"at least one valid snippet. Please include snippets for this edition."
        ),
        RejectionCode.SNIPPET_COUNT: lambda r: (
            f"Edition {r.edition_id} has {r.data['count']} snippet(s); "
            f"must be between 1 and 5 inclusive."
        ),
        RejectionCode.NO_MATCH: lambda r: (
            f"Snippet text was not found in any chunk for edition {r.edition_id}. "
            f"Ensure the text (and lead/trail, if using elision) is copied verbatim "
            f"from the chunk, and that non-empty text appears on both sides of '//...//`."
        ),
        RejectionCode.WORD_LIMIT: lambda r: (
            f"Resolved snippet has {r.data['word_count']} words, exceeding the "
            f"150-word hard limit (100-word target + 50-word grace margin). "
            f"Please shorten or use tighter elision."
        ),
        RejectionCode.ITEM_ID_MISMATCH: lambda r: (
            f"Snippet matched a chunk with item_id {r.data['matched_item_id']}, "
            f"not the claimed item_id {r.data['item_id']}. "
            f"Use the item_id shown in the search result for this chunk."
        ),
        RejectionCode.HALLUCINATED_EDITION: lambda r: (
            f"edition_id {r.edition_id} was not found in the search results "
            f"for tool call '{search_tool_call_id}'. "
            f"No snippets should have been submitted for this edition_id."
        ),
    }

    lines = [f"REJECTED: {len(rejected)} snippet(s) could not be stored."]
    for r in rejected:
        lines.append(
            f"  Edition {r.edition_id}{_snippet_preview(r)}: {_MESSAGES[r.code](r)}"
        )
    lines.append(
        "\nPlease resubmit select_relevant_snippets with corrected versions "
        "of the rejected snippets."
    )
    return "\n".join(lines)


class SnippetItem(TypedDict):
    item_id: int
    snippet: str


class EditionSnippets(TypedDict):
    edition_id: int
    snippets: List[SnippetItem]


@function_tool  # TODO: add @timer
@dynamic_docstring(SELECT_RELEVANT_SNIPPETS_DOC)
def select_relevant_snippets(
    ctx: ToolContext[SnippetsExecutionContext],  # TODO: change type
    snippets: List[EditionSnippets],
) -> str:
    # Returns:
    #     A summary of rejected snippets with the
    #     reason for rejection. If any snippets were rejected the response asks
    #     for a corrected resubmission.

    try:
        logger.info(f"{ctx.tool_name} tool called with arguments'{ctx.tool_arguments}'")
        # TODO: make a name and args callback for any tool, and add a edition_id log message to search_book, add traceback log to this callback

        # Build lookup: edition_id (int) -> edition_data entry
        edition_entry_map = {
            entry["orm_edition"].id: entry
            for entry in ctx.context.search_result.get("edition_data", [])
        }
        snippets_dict = {entry["edition_id"]: entry["snippets"] for entry in snippets}
        search_tool_call_id = ctx.context.search_tool_call_id

        total_snippets_submitted = sum(len(v) for v in snippets_dict.values())
        logger.info(
            f"select_relevant_snippets: {len(snippets_dict)} edition(s) submitted, "
            f"{total_snippets_submitted} total snippet(s), "
            f"{len(edition_entry_map)} edition(s) in search result."
        )

        rejected: List[Rejection] = []

        # ── Primary loop: edition_entry_map is authoritative ─────────────────────
        for edition_id, entry in edition_entry_map.items():
            # Case 1: already has saved snippets from a prior call — nothing to do.
            if entry.get("snippets"):
                continue

            submitted = snippets_dict.get(edition_id)

            # Case 2: no snippets saved and none submitted — flag as missing.
            if submitted is None:
                logger.debug(
                    f"Edition {edition_id} has no saved snippets and was not submitted."
                )
                rejected.append(
                    Rejection(edition_id, None, RejectionCode.MISSING_EDITION)
                )
                continue

            # Case 3: snippets submitted — validate and save.
            chunk_hits = entry["edition_hit"].get("chunk_hits", [])
            snippet_list = submitted

            # --- Validate edition: 1-5 snippets per edition ---
            if not (1 <= len(snippet_list) <= 5):
                logger.debug(
                    f"Rejecting edition {edition_id}: {len(snippet_list)} snippet(s) submitted; "
                    f"must be between 1 and 5 inclusive."
                )
                rejected.append(
                    Rejection(
                        edition_id,
                        None,
                        RejectionCode.SNIPPET_COUNT,
                        {"count": len(snippet_list)},
                    )
                )
                continue

            for snippet_obj in snippet_list:
                item_id = snippet_obj.get("item_id")
                snippet_text = snippet_obj.get("snippet", "")

                # --- Step 1: Find the first matching chunk ---
                matched_chunk = None
                resolved_snippet = None
                for chunk_hit in chunk_hits:
                    result = find_snippet_in_chunk(
                        snippet_text, chunk_hit.get("text", "")
                    )
                    if result is not None:
                        matched_chunk = chunk_hit
                        resolved_snippet = result
                        break

                if matched_chunk is None:
                    logger.debug(
                        f"Rejecting snippet for edition {edition_id}: not found in any chunk. "
                        f"snippet={repr(snippet_text[:60])}"
                    )
                    rejected.append(
                        Rejection(edition_id, snippet_obj, RejectionCode.NO_MATCH)
                    )
                    continue

                # --- Step 2: Validate word count on the resolved snippet ---
                word_count = len(resolved_snippet.split())
                if word_count > 150:
                    logger.debug(
                        f"Rejecting snippet for edition {edition_id}: resolved snippet "
                        f"has {word_count} words, exceeding 150-word hard limit."
                    )
                    rejected.append(
                        Rejection(
                            edition_id,
                            snippet_obj,
                            RejectionCode.WORD_LIMIT,
                            {"word_count": word_count},
                        )
                    )
                    continue

                # --- Step 3: Verify snippet item_id ---
                matched_item_id = matched_chunk.get("item_id")
                if matched_item_id is None or matched_item_id != item_id:
                    logger.debug(
                        f"Rejecting snippet for edition {edition_id}: claimed item_id={item_id} "
                        f"does not match chunk item_id={matched_item_id}."
                    )
                    rejected.append(
                        Rejection(
                            edition_id,
                            snippet_obj,
                            RejectionCode.ITEM_ID_MISMATCH,
                            {"item_id": item_id, "matched_item_id": matched_item_id},
                        )
                    )
                    continue

                # All validations passed — save into the entry directly.
                entry.setdefault("snippets", []).append(
                    {
                        "text": resolved_snippet,
                        "start_page": matched_chunk.get("start_page")
                        or matched_chunk.get("chunk_start_page"),
                        "end_page": matched_chunk.get("end_page")
                        or matched_chunk.get("chunk_end_page"),
                        "item_id": matched_chunk.get("item_id"),
                        "chunk_score": matched_chunk.get("score")
                        or matched_chunk.get("meta", {}).get("score"),
                    }
                )
                print(
                    f"saved snippet {resolved_snippet[:50]}... to edition {edition_id}"
                )

        # NOTE: log and ignore if edition_id is submitted but isn't in search_result
        for edition_id in snippets_dict:
            if edition_id not in edition_entry_map:
                logger.debug(
                    f"NB: edition {edition_id} submitted but not found in search results "
                    f"for tool call '{search_tool_call_id}'."
                )

        # --- Build response message ---
        logger.info(f"select_relevant_snippets: {len(rejected)} rejection(s).")
        if not rejected:
            return "Snippets successfully saved!"

        return build_rejection_message(rejected, search_tool_call_id)

    except Exception as e:
        logger.exception(f"Error during {ctx.tool_name} tool execution.")
        raise e


def get_relevant_snippets(run_result: RunResult) -> Optional[RunResult]:
    """Run a snippet agent to select and store relevant text snippets for the last
    search result in run_result.

    Returns None if no search has been executed (search_results is empty).
    The agent updates run_result.context_wrapper.context.search_results in place.
    """
    system_prompt = asyncio.run(
        run_result.last_agent.get_system_prompt(run_result.context_wrapper.context)
    )

    # TODO: test that the main agent has only 1 tool (this is assumed in writing
    # the relevant snippet prompt)
    search_results = run_result.context_wrapper.context.search_results
    if not search_results:
        print("XXXX AGENT has no search results")  # logged in format_search_results
        return None

    # Select last search result
    search_tool_call_id, search_result = list(search_results.items())[-1]

    # NOTE: I probably don't need to add the whole search tool description,
    # just a high level on what semantic search is
    snippet_agent_prompt = f"""
        You are given a conversation between a researcher and a research assistant LLM.
        The research assistant LLM agent received the following instructions:
        {system_prompt}

        The research agent had the following search tool available:
        Name: {run_result.last_agent.tools[0].name}
        Description: {run_result.last_agent.tools[0].description}

        Your job is to select relevant snippets that will be displayed to the research tool user to help them understand what content relevant to their research interest is available in the books returned by their search.

        You will return relevant snippets for the last search tool call: search tool call id = {search_tool_call_id}.

        Use the select_relevant_snippets tool record the relevant snippets you select to show to the user.
        """

    snippet_agent = Agent[SnippetsExecutionContext](
        name="Relevant Snippets Agent",
        model=run_result.last_agent.model,
        instructions=snippet_agent_prompt,
        tools=[select_relevant_snippets],
    )

    # NOTE: the agent updates the main agent's context_wrapper.context.search_results in place
    return Runner.run_sync(
        snippet_agent,
        run_result.to_input_list(),
        context=SnippetsExecutionContext(
            search_tool_call_id=search_tool_call_id, search_result=search_result
        ),
        run_config=RunConfig(
            tracing_disabled=True, model_settings=ModelSettings(temperature=0.0)
        ),
    )


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
    Create lines of str for a text display of book and chunk search results.
    Chunk display order controlled by input data order.
    """

    # Display book level data
    base_indent = "  "
    # lines.append("BOOK INFORMATION:")
    # lines.append(f"EDITION {i}:")
    lines.append(indent(f"EDITION ID: {edition_id}", base_indent))
    # lines.append(indent(f"WORK ID: {orm_work.id}", base_indent))
    lines.append(indent(f"TITLE: {frbr_fields['title']}", base_indent))
    lines.append(indent(f"AUTHORS: {frbr_fields['author_names']}", base_indent))
    lines.append(indent(f"PUBLISHER: {frbr_fields['publisher_names']}", base_indent))
    lines.append(indent(f"DATE: {frbr_fields['pub_date']}", base_indent))
    lines.append(
        indent(f"SUBJECTS: {frbr_fields['subject_list']}", base_indent)
    )  # Does this need to be wrap()'ed to multi-line
    lines.append(indent(f"LANGUAGE: {frbr_fields['language_list']}", base_indent))
    # lines.append(indent(f"MAX SCORE: {edition_hit['agg_score']:.4f}", base_indent))
    lines.append(indent(f"FOUND {len(chunk_hits)} MATCHING CHUNKS:", base_indent))
    lines.append("")

    # Display chunk level information
    # MAYBE: sort chunks by score (bigger is better, missing last) and limit display
    for j, chunk_hit in enumerate(chunk_hits, 1):
        text = chunk_hit.get("text", "(No Text)")
        score = chunk_hit.get("score", 0)
        chunk_id = chunk_hit.get("doc_id", "unknown")
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

        # lines.append(indent("CHUNK INFORMATION:", base_indent * 2))
        lines.append(indent(f"CHUNK {j}:", base_indent * 2))
        # lines.append(indent(f"ID: {chunk_id}", base_indent * 3))
        lines.append(
            indent(f"ITEM ID: {chunk_hit['item_id']}", base_indent * 3)
        )  # currently an edition might include chunks from multiple items
        lines.append(indent(f"PAGE: {page_display}", base_indent * 3))
        # lines.append(indent(f"SCORE: {score:.4f}", base_indent * 3))
        lines.append(indent(f"TEXT:\n{wrap(text)}", base_indent * 3))
        lines.append("")

    return lines


# TODO: rely on sort order from edition (and nested chunks) as passed (add to doc str)
# TODO: move to agent.py
def verbose_display_editions(
    edition_data, search_tool_call_id=None, query=None, as_str=False
):
    """
    Print or return a formatted str containing an ordered list book search
    results and their associated text excerpts. For each book, metadata is
    displayed including title, subjects, publication date. For each text excerpt,
    page number and search score is displayed. Books are ordered by the input list.

    Args:
        edition_data: List of dicts containing 'orm_work', 'orm_edition', 'edition_hit'
        query: The search query string
        as_str: If True, return as string; otherwise print
    """
    if not edition_data:
        return "There are no results for your query."

    lines = []
    if query is not None:
        lines.append(f'QUERY: "{wrap(query)}"')
        lines.append("\n")

    if search_tool_call_id is not None:
        lines.append(f'SEARCH TOOL CALL ID: "{search_tool_call_id}"')
        lines.append("\n")

    for i, edition_entry in enumerate(edition_data, 1):
        orm_work = edition_entry["orm_work"]
        orm_edition = edition_entry["orm_edition"]
        edition_hit = edition_entry["edition_hit"]
        # Format work and edition metadata
        frbr_fields = format_frbr_fields(orm_work, orm_edition)

        # Get chunk hits for this edition
        chunk_hits = edition_hit.get("chunk_hits", [])

        lines = display_book(lines, frbr_fields, chunk_hits, orm_edition.id)

        lines.append("-" * 80)

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


# TODO: When we insert messages in context specifying book for content search \
# context, remove book level info from search response to save tokens.
def verbose_display_chunks(
    chunk_hits,
    edition_id,
    search_tool_call_id=None,
    query=None,
    as_str=False,
    frbr_fields=None,
):
    """
    Print or return a formatted str containing an ordered list book excerpts.
    Book metadata  including title, subjects, publication date is optionally
    displayed. For each text excerpt, page number and search score is displayed.
    Excerpts are ordered by the input list.

    Args:
        chunk_hits: List of chunk hit dictionaries
        query: The search query string
        as_str: If True, return as string; otherwise print
        frbr_fields: Optional dict of formatted FRBR fields for book context
    """
    if not chunk_hits:
        return "There are no results for your query."

    lines = []
    if query is not None:
        lines.append(f'QUERY: "{wrap(query)}"')
        lines.append("\n")

    if search_tool_call_id is not None:
        lines.append(f'SEARCH TOOL CALL ID: "{search_tool_call_id}"')
        lines.append("\n")

    lines = display_book(lines, frbr_fields, chunk_hits, edition_id)

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


def compact_display_editions(edition_data, query, as_str=False):
    """
    Display edition search results in compact format.

    Args:
        edition_data: List of dicts containing 'orm_work', 'orm_edition', 'edition_hit'
        query: The search query string
        as_str: If True, return as string; otherwise print
    """
    if not edition_data:
        return "There are no results for your query."

    lines = []
    lines.append(f'QUERY: "{wrap(query)}"')
    lines.append("RESULTS:")

    for i, edition_entry in enumerate(edition_data, 1):
        orm_work = edition_entry["orm_work"]
        orm_edition = edition_entry["orm_edition"]
        edition_hit = edition_entry["edition_hit"]
        title = orm_work.title or "(No Title)"
        chunk_hits = edition_hit.get("chunk_hits", [])

        # Truncate title if too long
        title_display = title[:60] + "..." if len(title) > 60 else title

        lines.append(
            f" {i:>3}:  ({edition_hit['agg_score']:.3f}) Ed:{orm_edition.id:<6} W:{orm_work.id:<6} [{len(chunk_hits)} chunks] - {title_display}"
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
