from dataclasses import dataclass, field
from pathlib import Path
import traceback
from typing import Dict
import uuid
import sys
import os
import asyncio
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
# from sqlalchemy.ext.asyncio import create_async_engine

# api code
from .search import Searcher, get_book_metadata, verbose_display
from ..utils import hit_to_dict

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
    try:
        print(f"LLM QUERY: {query}")
        # print(f"tool call id = {ctx.tool_call_id}")

        # Execute vector search
        search_obj = ctx.context.searcher.vector_search(query, topk=100)
        # TODO: s.params(track_total_hits=True)

        if not len(search_obj):  # search_obj.total_hits
            return "No results found for your query."

        # Group chunk level hits by edition
        # Sort by score (as returned from ES)

        record_ids = set(hit.book_id for hit in search_obj)

        mapper = map_record_id_to_edition_id(record_ids)

        # hit = {meta.score, meta.id text, book_id, chunk_start_page, chunk_end_page}
        # TODO: add edition_id, work_id, item_id

        # group by edition and sort at the ES hit level (before adding FRBR data)

        # chunk hits
        hits = []
        for hit in search_obj:
            hit = hit_to_dict(hit)
            # hit['extra']['edition']['chunk_hits']. text, score, page_start, page_end
            hits.append(hit)

        # Join work/edition/item ids to hits
        hits = pd.merge(
            pd.DataFrame(hits), df, left_on="book_id", right_on="record_id"
        ).to_records()

        # edition hits
        # group chunk_hits by edition and sort (by...)

        # TEMP: we are limiting results to top 10 editions
        edition_ids = edition_ids[10:]

        from managers import DBManager
        from ..db import DBClient
        from ..utils import APIUtils

        with DBClient(
            DBManager(host=os.environ.get("POSTGRES_READ_HOST")).generate_engine()
        ) as db_client:
            # NOTE: the biggest difference btw VRA (current state) serach and DRB search is that VRA search does FRBR obj sorting of results outside/after ES and does not purely rely of ES search to determine search result order

            response_works = APIUtils.generate_response(
                db_client, hits, reader=reader_version, request=None, formats=None
            )

        # collapse_works_to_editions()

        # sort_editions_by_chunk_score()

        total_works = ...  # len(set(record['uuid'] for record in edition_data))

        # Group chunks by edition(item?)

        search_result = {
            "totalWorks": len(response_works),
            "works": response_works,
            # NOTE: paginated search not yet implemented
            "paging": APIUtils.formatPagingOptions(
                page=1, pageSize=search_obj.size, totalHits=total_hits
            ),
            "facets": {},
            # Q: is this needed for VRA?
            "searchParams": json.loads(ctx.tool_arguments),
        }
        # Store raw results in context for later use
        # ADD ticket to sprint..... on grouping results by book
        ctx.context.search_results[ctx.tool_call_id] = search_result

        # ALT : just send the JSON to the model (simpler than saving the raw results separately)
        # Q: return enriched hits as JSON? (add page)
        return verbose_display(enriched_hits, query, as_str=True)
    except Exception as e:
        traceback.print_exc(file=sys.stdout)
        raise e


def search_in_book():
    search_obj = ctx.context.searcher.vector_search(
        query, topk=100, filter={"term": {"book_id": ctx.context.item_id}}
    )

    # get metadata for single edition id


# TODO: context is getting overloaded, serverWorkerContext, executionContext, http method context....
# rename http method context to conversation_type?


def update_chat(conversation, context, item_id=None):
    """
    Send a message to the conversation and get the agent's response.

    The raw search results will be available in self.context.search_data
    for any post-processing or enrichment needed.

    Args:
        user_message: The user's message/query.

    Returns:
        The agent's RunResult obj.
    """

    assert context in ["contentSearch", "catalogSearch"]
    # MAYBE: other data validation approach?

    if context == "contentSearch":
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

    if context == "catalogSearch":
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

    messages = run_result.new_items
    # TODO: if  multiple search results/tool calls per update... merge into a single result
    result = list(context.search_results.values())[0]

    {
        "messages": messages,
        "result": result,
    }
