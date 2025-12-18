from dataclasses import dataclass, field
from pathlib import Path
import traceback
from typing import Dict
import uuid
import sys
import os
import asyncio

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

# shared code
from vector_indexing.embedding import GoogleEmbedder


# instantiate at module level (agent worker context)
# - execution context (searcher...)
# - event loop
# - read system prompt


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


@dataclass
class ExecutionContext:
    """Container used to inject objects into each agent run execution."""

    searcher: Searcher
    search_data: Dict = field(default_factory=dict)


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

        # Execute vector search using searcher from context
        search_obj = ctx.context.searcher.vector_search(query, topk=100)

        # get book metadata and enrich hits with that...
        enriched_hits = get_book_metadata(record_ids)

        # Store raw results in context for later use
        # ADD ticket to sprint..... on grouping results by book
        ctx.context.search_data[ctx.tool_call_id] = {
            "enriched_hits": enriched_hits,
            "query": query,  # TODO: all args
        }

        # Format results for the LLM
        if not enriched_hits:
            return "No results found for your query."

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

    output = run_result.new_items
    results = context.search_results

    {
        "output": output,
        "results": results,
    }
