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
)
from agents.tool_context import ToolContext
from agents.extensions.memory import SQLAlchemySession
from openai.types.shared import Reasoning
from sqlalchemy import text

from api.db import get_frbr_data_by_edition
# from sqlalchemy.ext.asyncio import create_async_engine

# api code
from .search import Searcher, get_book_metadata, verbose_display
from ..utils import APIUtils, hit_to_dict

# shared code
from vector_indexing.embedding import GoogleEmbedder
from utils.utils import create_sql_engine
from managers.db import DBManager


# instantiate at module level (agent worker context)
# - execution context (searcher...)
# - event loop
# - read system prompt


# MAYBE: use flask.current_app
# ALT: ApiWorkerContext
class AssistantWorkerContext:
    def __init__(self, index_name):
        # TODO: get embedder from index_name config
        self.searcher = Searcher(index_name=index_name, embedder=GoogleEmbedder())

        # Load system prompts
        SYSTEM_PROMPT_PATH = (
            Path(__file__).parent.parent.parent / "prompts" / "agent" / "1.md"
        )
        SYSTEM_PROMPT = SYSTEM_PROMPT_PATH.read_text()

        # Create a persistent event loop to use across SQLAlchemySession & the agent run
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)  # Q: is this necessary here?

        sql_engine = DBManager().generate_engine()


@dataclass
class ExecutionContext:
    """Container used to inject objects into each agent run execution."""

    searcher: Searcher
    search_results: Dict = field(default_factory=dict)


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

    with sql_engine.connect() as conn:
        result = conn.execute(query, {"record_ids": record_ids})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # in the long term we must index books as items, as records have no defined/guarenteed relationship to editions/items
    assert len(df) == len(record_ids), "record->edition is not 1->1"

    return df.to_dict(orient="records")  # figure out rid->eid map


@function_tool
def search_library(
    # ctx: RunContextWrapper[ConversationContext],
    ctx: ToolContext[ExecutionContext],
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

    PAGE_SIZE = 10

    # MAYBE: turn the below into 2 functions: group_by_edition_and_sort() and enrich_edition_hits()

    try:
        print(f"LLM QUERY: {query}")
        # print(f"tool call id = {ctx.tool_call_id}")

        # Execute vector search
        # take top 100 chunks and group by edition (then take top 10 editions)
        search_obj = ctx.context.searcher.vector_search(query, topk=100)
        # TODO: s.params(track_total_hits=True)

        if not len(search_obj):  # ? search_obj.total_hits
            return "No results found for your query."

        # chunk hits
        chunk_hits = []
        for chunk_hit in search_obj:
            chunk_hit = hit_to_dict(chunk_hit)
            chunk_hits.append(chunk_hit)

        # Join work/edition/item ids to chunk hits
        record_ids = set(hit["book_id"] for hit in chunk_hits)
        mapper = map_record_id_to_edition_id(record_ids)
        for chunk_hit in chunk_hits:
            chunk_hit.update(**mapper[chunk_hit.book_id])

        # hit = {meta.score, meta.id, text, book_id, chunk_start_page, chunk_end_page,  edition_id, work_id, item_id}

        # group by edition and sort at the ES hit level (before adding FRBR data)

        # Group chunk level hits by Edition
        # edition hits
        edition_hits = {}
        for chunk_hit in chunk_hits:
            if chunk_hit.edition_id not in edition_hits:
                edition_hits[chunk_hit.edition_id] = {
                    "work_id": chunk_hit.work_id,
                    "edition_id": chunk_hit.edition_id,
                    "chunk_hits": [chunk_hit],
                }
            else:
                edition_hits[chunk_hit.edition_id]["chunk_hits"].append(chunk_hit)

        # Sort editions (by aggregate chunk score)
        # max chunk score
        def sort_key(edition_hit):
            scores = [h["meta"]["score"] for h in edition_hit["chunk_hits"]]
            return max(scores)

        # # mean chunk score
        # def sort_key(edition_row):
        #     scores = [h['meta']['score'] for h in edition_hit['chunk_hits']]
        #     return np.mean(scores)

        edition_hits = sorted(edition_hits, key=sort_key)

        # TODO: if fewer than 10 editions, re-query more chunks until 10 editions are retrieved

        # Limit results to top 10 editions
        logger.info(
            f"{len(edition_hits)} editions retrieved by vector search of top {100} chunks"
        )
        logger.info("limiting results to top 10 editions")
        edition_hits = edition_hits[PAGE_SIZE:]
        # TODO: handle paginating or providing more edition hits

        # Fetch FRBR data (from DB) for editions in search result (page)
        edition_ids = [h.edition_id for h in edition_hits]
        frbr_data = get_frbr_data_by_edition(edition_ids)

        # Merge ES hit data and FRBR metadata (maintaining edition hit sort order)
        edition_data = []  # (ORM work, ORM edition, ES edition_hit)
        for edition_hit in edition_hits:
            orm_work, orm_edition = [
                (w, e) for w, e in frbr_data if e.id == edition_hit.edition_id
            ][0]
            edition_data.append((orm_work, orm_edition, edition_hit))

        # NOTE: the biggest difference btw VRA (current state) search and DRB search is that VRA search does FRBR obj sorting of results outside/after ES and does not purely rely on ES search to determine search result order

        # Format editions for API response
        # MAYBE: save raw merged FRBR and ES data and format in view func
        response_editions = ...
        search_result = {
            "edition_data": response_editions,
            # NOTE: paginated search not yet implemented, only 1 fixed result set size
            "paging": APIUtils.formatPagingOptions(
                page=1, pageSize=PAGE_SIZE, totalHits=PAGE_SIZE
            ),
            "search_params": json.loads(ctx.tool_arguments),
        }
        # Store API formatted json results for later reference
        ctx.context.search_results[ctx.tool_call_id] = search_result

        # Format editions for LLM
        # ALT : convert edition data to json and send (full) JSON to LLM (simpler than saving JSON/API response separately)
        return verbose_display(edition_data, query, as_str=True)

    except Exception as e:
        logger.exception("Error during search_library tool execution.")
        raise e


def search_in_book():
    search_obj = ctx.context.searcher.vector_search(
        query, topk=100, filter={"term": {"book_id": ctx.context.item_id}}
    )

    # get metadata for single edition id


def update_chat(conversation, conversation_type, item_id=None):
    """
    Send a message to the conversation and get the agent's response.

    The raw search results will be available in self.context.search_data
    for any post-processing or enrichment needed.

    Args:
        user_message: The user's message/query.

    Returns:
        The agent's RunResult obj.
    """

    assert conversation_type in ["contentSearch", "catalogSearch"]
    # MAYBE: other data validation approach?

    # Search for books
    if conversation_type == "contentSearch":
        assert item_id is not None, (
            'item_id is required for conversation_type="contentSearch"'
        )

        # TODO: add catalog search context type
        exec_context = ExecutionContext(
            searcher=WORKER_CONTEXT.searcher, item_id=item_id
        )

        CONTENT_SEARCH_SYSTEM_PROMPT

        # Instantiate the content search agent (unique system prompt and tools)
        self.agent = Agent[ExecutionContext](
            name="Research Library Assistant",
            model="litellm/gemini/gemini-2.5-flash",
            # model_settings=ModelSettings(
            #     # include_usage=True,  # only for chatcompletions based agents/models # requires openai model?
            #     # reasoning=Reasoning(effort="low"),  # converted to chat completions API  reasoning_effort= which  is consistently supported in litellm
            # ),
            instructions=CONTENT_SEARCH_SYSTEM_PROMPT,
            tools=[search_library],
        )

    # Search within single book
    if conversation_type == "catalogSearch":
        exec_context = ExecutionContext(searcher=WORKER_CONTEXT.searcher)

        CATALOG_SEARCH_SYSTEM_PROMPT

        # Instantiate the catalog search agent (unique system prompt and tools)
        self.agent = Agent[ExecutionContext](
            name="Research Library Assistant",
            model="litellm/gemini/gemini-2.5-flash",
            # model_settings=ModelSettings(
            #     # include_usage=True,  # only for chatcompletions based agents/models # requires openai model?
            #     # reasoning=Reasoning(effort="low"),  # converted to chat completions API  reasoning_effort= which  is consistently supported in litellm
            # ),
            instructions=CATALOG_SEARCH_SYSTEM_PROMPT,
            tools=[search_in_book],
        )

    async def _run():
        # Run the agent using the persistent loop's session which will be \
        # used to call awaitable asyncpg/SQLAlchemySession instance methods bound to that loop
        return await Runner.run(self.agent, conversation, context=exec_context)

    run_result = self._loop.run_until_complete(_run())

    # Extract new messages in conversation
    messages = run_result.new_items

    # Extract (single) search tool result
    result = list(conversation_type.search_results.values())[0]
    # TODO: if  multiple search results/tool calls per update... merge into a single result
    if len(conversation_type.search_results) > 1:
        logger.warning(
            f"{len(len(conversation_type.search_results))} tool calls during agent response."
        )

    return {
        "messages": messages,
        "result": result,
    }
