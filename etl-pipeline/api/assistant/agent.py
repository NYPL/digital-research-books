from dataclasses import dataclass, field
import json
from pathlib import Path
import traceback
from typing import Dict
import uuid
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
from sqlalchemy import text

from api.blueprints.chat import INDEX_NAME
from api.db import get_frbr_data_by_edition, Session
# from sqlalchemy.ext.asyncio import create_async_engine

# api code
from .search import Searcher, verbose_display_editions, verbose_display_chunks
from ..utils import APIUtils, hit_to_dict

# shared code
from vector_indexing.embedding import GoogleEmbedder
from utils.utils import create_sql_engine
from managers.db import DBManager
from logger import create_log


logger = create_log(__name__)


# instantiate at module level (agent worker context)
# - execution context (searcher...)
# - event loop
# - read system prompt

# max number of editions to return from catalog search
PAGE_SIZE = 10

INDEX_NAME = "vra_chunks_gemini-embedding-001"

PROMPTS_DIR = Path(__file__).parent.parent.parent / "prompts"


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
    search_results: Dict = field(default_factory=dict)
    item_id: int


def map_record_id_to_edition_id(record_ids):
    # record_id -> edition id

    query = text("""
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
        r.id IN :record_ids;
    """)

    with Session() as session:
        # NOTE: Session() has all the same methods as engine.connect(). see: https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#unitofwork-transaction
        result = session.execute(query, {"record_ids": record_ids})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # we expect records = editions = items for GRIN books
    # in the long term we must index books as items, as records have no \
    # defined/guaranteed relationship to editions/items
    assert len(df) == len(record_ids), "record->edition is not 1->1"

    # record_id -> {id -> value, ...}
    return df.set_index("record_id").to_dict(orient="index")


@function_tool
def search_library(
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

    try:
        print(f"LLM QUERY: {query}")
        # print(f"tool call id = {ctx.tool_call_id}")

        # Execute vector search
        # take top 100 chunks and group by edition (then take top 10 editions)
        search_obj = ctx.context.searcher.vector_search(query, topk=100)
        # TODO: s.params(track_total_hits=True)

        if not len(search_obj):  # ? search_obj.total_hits
            return "No results found for your query."

        # MAYBE: turn the below into 2 functions: group_by_edition_and_sort() and enrich_edition_hits() (with limit to top 10 in between)

        # chunk hits
        chunk_hits = []
        for chunk_hit in search_obj:
            chunk_hit = hit_to_dict(chunk_hit)
            chunk_hits.append(chunk_hit)

        # Join work/edition/item ids to chunk hits
        # TODO: FUTURE: index books with book_id=edition_id
        record_ids = set(hit["book_id"] for hit in chunk_hits)
        record_id2frbr_ids = map_record_id_to_edition_id(record_ids)
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
            f"{len(edition_hits)} editions retrieved by vector search of top {100} chunks"
        )
        logger.info("limiting results to top 10 editions")
        edition_hits = edition_hits[:PAGE_SIZE]
        # TODO: handle paginating or providing more edition hits

        # Fetch FRBR data (from DB) for editions in search result (page)
        edition_ids = [h["edition_id"] for h in edition_hits]
        frbr_data = get_frbr_data_by_edition(edition_ids)

        # Merge ES hit data and FRBR metadata (maintaining edition sort order)
        edition_data = []  # (ORM work, ORM edition, ES edition_hit)
        for edition_hit in edition_hits:
            orm_work, orm_edition = [
                (w, e) for w, e in frbr_data if e.id == edition_hit["edition_id"]
            ][0]
            edition_data.append((orm_work, orm_edition, edition_hit))
        # ALT: if frbr_data was pre-sorted by edition_id ordering, we could \
        # iterate over frbr_data and lookup (rather than loop) matching es data \
        # from an edition_hits dict

        # NOTE: the biggest difference btw VRA (current state) search and DRB \
        # search (in terms of output formatting) is that VRA search does FRBR obj \
        # sorting outside/after ES and does not purely rely on ES search to \
        # determine results ordering

        # Store search results for later reference
        ctx.context.search_results[ctx.tool_call_id] = {
            "edition_data": edition_data,  # ordered search result
            "search_params": json.loads(ctx.tool_arguments),
        }

        # Format editions for LLM (markdown)
        # ALT : convert edition data to json and send (full) JSON to LLM (simpler \
        # than saving JSON/API response separately but edition data json may \
        # include irrelevant metadata)
        return verbose_display_editions(edition_data, query, as_str=True)

    except Exception as e:
        logger.exception("Error during search_library tool execution.")
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
        print(f"LLM QUERY (in-book): {query}")

        # Execute vector search filtered to single book
        search_obj = ctx.context.searcher.vector_search(
            query, topk=10, filter_query={"term": {"book_id": ctx.context.item_id}}
        )

        if not len(search_obj):
            return "No results found for your query in this book."

        # Convert chunk hits to dict format
        chunk_hits = []
        for chunk_hit in search_obj:
            chunk_hit = hit_to_dict(chunk_hit)
            chunk_hits.append(chunk_hit)

        # NOTE: getting FRBR data is only for LLM context, TODO: inject in dynamic system prompt instead

        # Map record_id to edition/work ids for the single book
        record_ids = set(hit["book_id"] for hit in chunk_hits)
        record_id2frbr_ids = map_record_id_to_edition_id(record_ids)
        for chunk_hit in chunk_hits:
            chunk_hit.update(**record_id2frbr_ids[chunk_hit["book_id"]])

        # Fetch FRBR data for the book
        # Should only be one edition since we're searching within one book
        edition_ids = list(set(hit["edition_id"] for hit in chunk_hits))
        frbr_data = get_frbr_data_by_edition(edition_ids)
        # Get the single work/edition pair
        orm_work, orm_edition = frbr_data[0]

        # Store search results for later reference
        ctx.context.search_results[ctx.tool_call_id] = {
            "chunk_hits": chunk_hits,
            # "work": orm_work,
            # "edition": orm_edition,
            "search_params": json.loads(ctx.tool_arguments),
        }

        # Format results for LLM
        return verbose_display_chunks(
            chunk_hits, query, as_str=True, work=orm_work, edition=orm_edition
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


def update_chat(conversation, conversation_type, item_id=None) -> RunResult:
    """
    Send a message to the conversation and get the agent's response.

    The raw search results will be available in self.context.search_data
    for any post-processing or enrichment needed.

    Args:
        user_message: The user's message/query.

    Returns:
        The agent's RunResult obj.
    """

    # TODO: figure out how to do thread safe  module level instantiations for \
    # some reused objs (searcher, system prompts, async loop, etc...) (for sharing btw server \
    # request workers/threads)

    # TODO: get embedder from index_name config
    searcher = Searcher(index_name=INDEX_NAME, embedder=GoogleEmbedder())

    # Search within single book
    if conversation_type == "contentSearch":
        exec_context = ContentSearchExecutionContext(searcher=searcher, item_id=item_id)
        system_prompt = (PROMPTS_DIR / "agent" / "content_search.md").read_text()
        tools = [search_in_book]

    # Search for books in catalog
    elif conversation_type == "catalogSearch":
        exec_context = CatalogSearchExecutionContext(searcher=searcher)
        system_prompt = (PROMPTS_DIR / "agent" / "1.md").read_text()
        tools = [search_library]

    run_result = run_agent(exec_context, system_prompt, tools, conversation)

    return run_result
