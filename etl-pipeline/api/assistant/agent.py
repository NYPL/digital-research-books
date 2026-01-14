from dataclasses import dataclass, field
import json
from pathlib import Path
import traceback
from typing import Dict
import uuid
from textwrap import indent
import sys
import os
import asyncio
import numpy as np
import pandas as pd

from agents import (
    Agent,
    Runner,
    function_tool,
    RunContextWrapper,
    SQLiteSession,
    ModelSettings,
    RunResult,
)
from agents.tool_context import ToolContext
from agents.extensions.memory import SQLAlchemySession
from openai.types.shared import Reasoning
from sqlalchemy import text, bindparam
from jinja2 import Template

from utils.chunker import chunk


# from sqlalchemy.ext.asyncio import create_async_engine

# api code
from .search import Searcher
from ..utils import APIUtils, hit_to_dict, remove_markdown_comments
from ..db import get_frbr_data_by_edition, Session

# shared code
from vector_indexing.embedding import GoogleEmbedder
from utils.utils import create_sql_engine
from managers.db import DBManager
from logger import create_log
from utils.utils import wrap


logger = create_log(__name__)


# instantiate at module level (agent worker context)
# - execution context (searcher...)
# - event loop
# - read system prompt

# max number of editions to return from catalog search
PAGE_SIZE = 10

INDEX_NAME = (
    "vra_chunks_gemini-embedding-001"  # imported into blueprints/chat.py # Q: move?
)

PROMPTS_DIR = Path(__file__).parent / "prompts"


MODEL = "litellm/gemini/gemini-2.5-flash"


@dataclass
class CatalogSearchExecutionContext:
    """Container used to inject objects into each agent run execution."""

    searcher: Searcher
    search_results: Dict = field(default_factory=dict)


@dataclass
class ContentSearchExecutionContext:
    """Container used to inject objects into each agent run execution."""

    searcher: Searcher
    edition_id: int
    search_results: Dict = field(default_factory=dict)
    frbr_fields: Dict = field(default_factory=dict)


def map_editions_and_records(record_ids=None, edition_ids=None):
    if record_ids:
        bind_param, table_alias, ids = "record_ids", "r", record_ids
    elif edition_ids:
        assert record_ids is None, "both record_ids and edition_ids are non-null"
        bind_param, table_alias, ids = "edition_ids", "e", edition_ids

    query = text(f"""
    SELECT
        r.id AS record_id,
        i.id as item_id,
        e.id AS edition_id
    FROM
        records r
    JOIN
        items i ON r.id = i.record_id
    JOIN
        editions e ON i.edition_id = e.id
    WHERE
        {table_alias}.id IN :{bind_param};
    """).bindparams(bindparam(bind_param, expanding=True))
    # Discussion of this bind param issue: https://stackoverflow.com/questions/13190392/how-can-i-bind-a-list-to-a-parameter-in-a-custom-query-in-sqlalchemy

    with Session() as session:
        # NOTE: Session() has all the same methods as engine.connect(). see: https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#unitofwork-transaction
        result = session.execute(query, {bind_param: list(ids)})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # we expect records = editions = items for NYPL GRIN books
    # in the long term we must index books as items, as records have no \
    # defined/guaranteed relationship to editions/items

    # Validate 1-to-1 mapping
    source_col = bind_param.rstrip("s")
    target_col = "edition_id" if bind_param == "record_ids" else "record_id"

    requested_ids = set(ids)
    found_source_ids = set(df[source_col])
    print("XXX", target_col, df[target_col])

    # Check for missing source IDs
    missing_source_ids = requested_ids - found_source_ids

    # Check for duplicate source IDs (one source -> multiple targets)
    source_value_counts = df[source_col].value_counts()
    duplicate_sources = source_value_counts[source_value_counts > 1].index.tolist()

    # Check for duplicate target IDs (multiple sources -> one target)
    target_value_counts = df[target_col].value_counts()
    duplicate_targets = target_value_counts[target_value_counts > 1].index.tolist()

    if missing_source_ids or duplicate_sources or duplicate_targets:
        error_parts = [f"{source_col}->{target_col} mapping is not 1->1:"]
        if missing_source_ids:  # TODO: this is that the source col ids are either not present in DB or not mapped to targets
            error_parts.append(f"Missing {source_col}: {sorted(missing_source_ids)}")
        if duplicate_sources:
            error_parts.append(
                f"Duplicate {source_col} (matched multiple {target_col}s): {sorted(duplicate_sources)}"
            )
        if duplicate_targets:
            error_parts.append(
                f"Duplicate {target_col}s (matched multiple {source_col}): {sorted(duplicate_targets)}"
            )
        raise AssertionError("\n".join(error_parts))

    # source id -> {target id -> value, ...}
    return df.set_index(source_col).to_dict(orient="index")


@function_tool
def search_library_catalog(
    # ctx: RunContextWrapper[ConversationContext],
    ctx: ToolContext[CatalogSearchExecutionContext],
    query: str,
) -> str:
    """
    Search the research library catalog for relevant book sections.
    This tool uses semantic vector search on a vector index composed of all
    the books broken into short text chunks with each chunk embedded with a
    semantic embedding model. The vector search tool returns text chunks with
    the most relevant semantic content.

    Args:
        query: The query will be embedded with a semantic embedding model and
        used for the vector similarity search.

    Returns:
        A formatted string containing search results with book titles, page numbers,
        subjects, dates, and text excerpts.
    """
    # TODO: doc string - guidance on reformatting the user query to an appropriate search query

    try:
        logger.info(f"{ctx.tool_name} tool called with args: '{ctx.tool_arguments}'")

        # Execute vector search
        # take top 100 chunks and group by edition (then take top 10 editions)
        resp = ctx.context.searcher.vector_search(query, topk=100)
        logger.info(f"Retrieved {len(resp.hits)} chunk hits from Elasticsearch")
        # TODO: s.params(track_total_hits=True)

        if not len(resp.hits):  # ? search_obj/resp.total_hits
            return "No results found for your query."

        # MAYBE: turn the below into 2 functions: group_by_edition_and_sort() and enrich_edition_hits() (with limit to top 10 in between)

        # chunk hits
        chunk_hits = []
        for chunk_hit in resp.hits:
            chunk_hit = hit_to_dict(chunk_hit)
            # TEMP
            # TODO: ensure all books are indexed with book_id # TODO: gracebully handle book_id where accessed
            if not chunk_hit.get("book_id"):
                logger.error(
                    f"Chunk missing book_id: id={chunk_hit['meta'].get('id')}, keys={chunk_hit.keys()}"
                )
            else:
                chunk_hits.append(chunk_hit)
        print("XXXXX", json.dumps(chunk_hits, indent=2))

        # convert record_ids to edition_ids to fetch FRBR data
        # Join work/edition/item ids to chunk hits
        # TODO: FUTURE: index books with book_id=edition_id so we don't have to convert
        record_ids = set(hit["book_id"] for hit in chunk_hits)
        record_id2frbr_ids = map_editions_and_records(record_ids=record_ids)
        for chunk_hit in chunk_hits:
            chunk_hit.update(**record_id2frbr_ids[chunk_hit["book_id"]])
        # chunk_hit = {meta.score, meta.id, text, book_id, chunk_start_page, chunk_end_page,  edition_id, work_id, item_id}

        # Group ES Chunk hits by Edition (before adding FRBR data)
        # edition hits
        edition_hits = {}
        for chunk_hit in chunk_hits:
            if chunk_hit["edition_id"] not in edition_hits:
                edition_hits[chunk_hit["edition_id"]] = {
                    "work_id": chunk_hit["work_id"],
                    "edition_id": chunk_hit["edition_id"],
                    "chunk_hits": [chunk_hit],
                }
            else:
                edition_hits[chunk_hit["edition_id"]]["chunk_hits"].append(chunk_hit)

        # TODO: if fewer than 10 editions, re-query more chunks until 10 editions are retrieved

        # Sort editions (by aggregate chunk score)
        # max chunk score
        def sort_key(edition_hit):
            scores = [h["meta"]["score"] for h in edition_hit["chunk_hits"]]
            return max(scores)

        # # mean chunk score
        # def sort_key(edition_row):
        #     scores = [h['meta']['score'] for h in edition_hit['chunk_hits']]
        #     return np.mean(scores)

        edition_hits = sorted(
            edition_hits.values(), key=sort_key
        )  # ALT: retain dict structure

        # Limit results to the 10 top scoring editions
        logger.info(
            f"Aggregated {len(edition_hits)} editions from {len(chunk_hits)} chunk hits"
        )
        logger.info(f"Limiting results to top {PAGE_SIZE} editions")
        edition_hits = edition_hits[:PAGE_SIZE]
        # TODO: handle paginating or providing more edition hits

        # Fetch FRBR data (from DB) for editions in search result (page)
        edition_ids = [h["edition_id"] for h in edition_hits]
        frbr_data = get_frbr_data_by_edition(edition_ids)
        # TODO: handle get_frbr_data_by_edition() returns empty

        # Merge ES hit data and FRBR metadata (maintaining edition sort order)
        edition_data = []  # dict with keys: orm_work, orm_edition, edition_hit
        for edition_hit in edition_hits:
            # match orm work/edition to ES edition hit
            orm_work, orm_edition = [
                (w, e) for w, e in frbr_data if e.id == edition_hit["edition_id"]
            ][0]
            edition_data.append(
                {
                    "orm_work": orm_work,
                    "orm_edition": orm_edition,
                    "edition_hit": edition_hit,
                }
            )
        # ALT: if frbr_data was pre-sorted by edition_id ordering, we could \
        # iterate over frbr_data and lookup (rather than loop) matching es data \
        # from an edition_hits dict

        # NOTE: the biggest difference btw VRA (current state) search and DRB \
        # search (in terms of output formatting) is that VRA search does FRBR obj \
        # sorting outside/after ES and does not purely rely on ES search to \
        # determine results ordering

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
        return verbose_display_editions(edition_data, query, as_str=True)

    except Exception as e:
        logger.exception("Error during search_library_catalog tool execution.")
        raise e

    # requesting frontend already has edition frbr metadata so only chunk hit ES data needed in API response, but... we do want to give the LLM that book level context when generating its response
    # include page num info


@function_tool
def search_in_book(
    ctx: ToolContext[ContentSearchExecutionContext],
    query: str,
) -> str:
    """
    Search within a specific book for relevant sections.
    This tool performs semantic vector search constrained to a XXXsingle book,
    returning the most relevant text chunks with their page numbers.

    Args:
        query: The query will be embedded with a semantic embedding model and
        used for the vector similarity search within the book.

    Returns:
        A formatted string containing search results with page numbers,
        scores, and text excerpts from the book.
    """

    try:
        logger.info(
            f"{ctx.tool_name} tool called with args: '{ctx.tool_arguments}' for edition_id: {ctx.context.edition_id}"
        )

        # Execute vector search filtered to single book
        resp = ctx.context.searcher.vector_search(
            query, topk=10, filter_query={"term": {"book_id": ctx.context.edition_id}}
        )
        logger.info(
            f"Retrieved {len(resp.hits)} chunk hits from Elasticsearch for book"
        )  # # Q: redudnant to searcher logging

        if not len(resp.hits):
            return "No results found for your query in this book."

        # Convert chunk hits to dict format
        chunk_hits = []
        for chunk_hit in resp.hits:
            chunk_hit = hit_to_dict(chunk_hit)
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
            chunk_hits, query, as_str=True, frbr_fields=ctx.context.frbr_fields
        )

    except Exception as e:
        logger.exception("Error during search_in_book tool execution.")
        raise e


def run_agent(exec_context, system_prompt, tools, conversation):
    # Create a persistent event loop to use across the agent run and potential derived other objs
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)

    agent = Agent[type(exec_context)](
        name="Research Library Assistant",  # necesary?
        model=MODEL,
        # model_settings=ModelSettings(
        #     # include_usage=True,  # only for chatcompletions based agents/models # requires openai model?
        #     # reasoning=Reasoning(effort="low"),  # converted to chat completions API  reasoning_effort= which  is consistently supported in litellm
        # ),
        instructions=system_prompt,
        tools=tools,
    )

    async def _run():
        # Run the agent using a centralized persistent loop session
        # this is not necessary here, but allows calling other related awaitable \
        # instance methods bound to that loop
        return await Runner.run(agent, conversation, context=exec_context)

    run_result = _loop.run_until_complete(_run())

    return run_result


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
    # some reused objs (searcher, system prompts, async loop, etc...) (for sharing btw server \
    # request workers/threads)

    # TODO: get embedder from index_name config
    searcher = Searcher(index_name=INDEX_NAME, embedder=GoogleEmbedder())

    # Search within single book
    if conversation_type == "contentSearch":
        # TEMP: convert edition_id to record_id to filter ES search
        record_id = map_editions_and_records(edition_ids=[edition_id])[edition_id][
            "record_id"
        ]

        # Fetch FRBR data for the book
        frbr_data = get_frbr_data_by_edition([edition_id])
        orm_work, orm_edition = frbr_data[0]
        frbr_fields = format_frbr_fields(orm_work, orm_edition)

        # NOTE: falsely naming record_id as edition_id to make future state a smaller refactor
        exec_context = ContentSearchExecutionContext(
            searcher=searcher, edition_id=record_id, frbr_fields=frbr_fields
        )

        template = Template((PROMPTS_DIR / "chat" / "1.jinja.md").read_text())
        system_prompt = template.render(
            conversation_type="content_search", frbr_fields=frbr_fields
        )
        tools = [search_in_book]

    # Search for books in catalog
    elif conversation_type == "catalogSearch":
        exec_context = CatalogSearchExecutionContext(searcher=searcher)
        template = Template((PROMPTS_DIR / "chat" / "1.jinja.md").read_text())
        system_prompt = remove_markdown_comments(
            template.render(conversation_type="catalog_search")
        )
        tools = [search_library_catalog]

    run_result = run_agent(exec_context, system_prompt, tools, conversation)

    return run_result


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

    return {
        "title": title,
        "author_names": author_names,
        "subject_list": subject_list,
        "pub_date": pub_date,
        "publisher_names": publisher_names,
    }


# TODO: rely on sort order from edition (and nested chunks) as passed (add to doc str)
# TODO: move to agent.py
def verbose_display_editions(edition_data, query, as_str=False):
    """
    Display edition search results with detailed information.

    Args:
        edition_data: List of dicts with keys 'orm_work', 'orm_edition', 'edition_hit'
                     where edition_hit contains work_id, edition_id, and chunk_hits
        query: The search query string
        as_str: If True, return as string; otherwise print
    """
    lines = []
    lines.append(f'QUERY: "{wrap(query)}"')
    lines.append("\n")

    for i, edition_entry in enumerate(edition_data, 1):
        orm_work = edition_entry["orm_work"]
        orm_edition = edition_entry["orm_edition"]
        edition_hit = edition_entry["edition_hit"]
        # Format work and edition metadata
        frbr_fields = format_frbr_fields(orm_work, orm_edition)

        # Get chunk hits for this edition
        chunk_hits = edition_hit.get("chunk_hits", [])

        # TODO: use the same function for chunk score agg as in group by edition, or... set the edition score in the edition_data
        max_score = (
            max([h.get("meta", {}).get("score", 0) for h in chunk_hits])
            if chunk_hits
            else 0
        )

        # Display work/edition data
        base_indent = "  "
        lines.append(f"EDITION {i}:")
        lines.append(
            indent(
                f"WORK ID: {orm_work.id} | EDITION ID: {orm_edition.id}", base_indent
            )
        )
        lines.append(indent(f"TITLE: {frbr_fields['title']}", base_indent))
        lines.append(indent(f"AUTHORS: {frbr_fields['author_names']}", base_indent))
        lines.append(
            indent(f"PUBLISHER: {frbr_fields['publisher_names']}", base_indent)
        )
        lines.append(indent(f"DATE: {frbr_fields['pub_date']}", base_indent))
        lines.append(
            indent(f"SUBJECTS: {frbr_fields['subject_list']}", base_indent)
        )  # Does this need to be wrap()'ed to multi-line
        lines.append(indent(f"MAX SCORE: {max_score:.4f}", base_indent))
        lines.append(indent(f"CHUNKS FOUND: {len(chunk_hits)}", base_indent))
        lines.append("")

        # Display chunk data (for this edition)
        # MAYBE: sort chunks by score and limit display
        for j, chunk_hit in enumerate(chunk_hits, 1):
            text = chunk_hit.get("text", "(No Text)")
            score = chunk_hit.get("meta", {}).get("score", 0)
            chunk_id = chunk_hit.get("meta", {}).get("id", "unknown")
            # Extract page range from chunk metadata
            start_page = chunk_hit.get("chunk_start_page")
            end_page = chunk_hit.get("chunk_end_page")
            if start_page is not None and end_page is not None:
                if start_page == end_page:
                    page_display = str(start_page)
                else:
                    page_display = f"{start_page}-{end_page}"
            else:
                page_display = "?"

            lines.append(indent(f"CHUNK {j}:", base_indent * 2))
            lines.append(indent(f"ID: {chunk_id}", base_indent * 3))
            lines.append(indent(f"PAGE: {page_display}", base_indent * 3))
            lines.append(indent(f"SCORE: {score:.4f}", base_indent * 3))
            lines.append(indent(f"TEXT:\n{wrap(text)}", base_indent * 3))
            lines.append("")

        lines.append("-" * 80)

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


def compact_display_editions(edition_data, query, as_str=False):
    """
    Display edition search results in compact format.

    Args:
        edition_data: List of dicts with keys 'orm_work', 'orm_edition', 'edition_hit'
                     where edition_hit contains work_id, edition_id, and chunk_hits
        query: The search query string
        as_str: If True, return as string; otherwise print
    """
    lines = []
    lines.append(f'QUERY: "{wrap(query)}"')
    lines.append("RESULTS:")

    for i, edition_entry in enumerate(edition_data, 1):
        orm_work = edition_entry["orm_work"]
        orm_edition = edition_entry["orm_edition"]
        edition_hit = edition_entry["edition_hit"]
        title = orm_work.title or "(No Title)"
        chunk_hits = edition_hit.get("chunk_hits", [])
        max_score = (
            max([h.get("meta", {}).get("score", 0) for h in chunk_hits])
            if chunk_hits
            else 0
        )

        # Truncate title if too long
        title_display = title[:60] + "..." if len(title) > 60 else title

        lines.append(
            f" {i:>3}:  ({max_score:.3f}) Ed:{orm_edition.id:<6} W:{orm_work.id:<6} [{len(chunk_hits)} chunks] - {title_display}"
        )

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)


def get_score(entry):
    return entry.get("meta", {}).get("score", float("-inf"))


# TODO: use dynamic system prompt to insert book level info into system prompt rather than each individual content search
def verbose_display_chunks(chunk_hits, query, as_str=False, frbr_fields=None):
    """
    Display chunk search results, optionally with FRBR book metadata.

    Args:
        chunk_hits: List of chunk hit dictionaries
        query: The search query string
        as_str: If True, return as string; otherwise print
        frbr_fields: Optional dict of formatted FRBR fields for book context
    """
    # Sort entries by ['meta']['score'] descending, missing scores last
    sorted_hits = sorted(chunk_hits, key=get_score, reverse=True)

    lines = []
    lines.append(f'QUERY: "{wrap(query)}"')
    lines.append("\n")

    # Display book context (if FRBR data provided)
    if frbr_fields:
        lines.append("BOOK INFORMATION:")
        lines.append(indent(f"TITLE: {frbr_fields['title']}", "  "))
        lines.append(indent(f"AUTHORS: {frbr_fields['author_names']}", "  "))
        lines.append(indent(f"DATE: {frbr_fields['pub_date']}", "  "))
        lines.append(indent(f"SUBJECTS: {frbr_fields['subject_list']}", "  "))
        lines.append("")
        lines.append(f"FOUND {len(sorted_hits)} MATCHING SECTIONS:")
        lines.append("-" * 80)

    # Display chunk level information
    for i, entry in enumerate(sorted_hits, 1):
        text = entry.get("text", "(No Text)")
        score = get_score(entry)

        # Extract page range from chunk metadata
        start_page = entry.get("chunk_start_page")
        end_page = entry.get("chunk_end_page")
        if start_page is not None and end_page is not None:
            if start_page == end_page:
                page_display = str(start_page)
            else:
                page_display = f"{start_page}-{end_page}"
        else:
            page_display = "?"

        lines.append(f"RESULT {i}:")
        lines.append(indent(f"ID: {entry['meta']['id']}", "  "))
        lines.append(indent(f"PAGE: {page_display}", "  "))
        lines.append(indent(f"SCORE: {score}", "  "))
        lines.append(indent("TEXT:", "  "))
        lines.append(indent(f"{wrap(text)}\n", "  "))
        lines.append("-" * 60)

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
            f" {i:>3}:  ({get_score(entry):.3f}) {entry['meta']['id']:<19} -  {entry['title']}"
        )

    msg = "\n".join(lines)
    if as_str:
        return msg
    else:
        print(msg)
