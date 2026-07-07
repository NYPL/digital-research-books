"""
Unit tests verifying that search_catalog and search_book surface tool
execution errors with the agents SDK's default tool-error prefix
(TOOL_ERROR_PREFIX), rather than raising or returning some other shape.

This matters because api/blueprints/result_reason.py detects tool errors
by checking `tool_call_output.startswith(TOOL_ERROR_PREFIX)` — if the SDK's
error format ever changes, that guard silently stops working.
"""

import json
from unittest.mock import MagicMock

import pytest

from agents.tool_context import ToolContext

from api.assistant.agent import (
    CatalogSearchExecutionContext,
    ContentSearchExecutionContext,
    TOOL_ERROR_PREFIX,
    search_book,
    search_catalog,
)

pytestmark = pytest.mark.asyncio


def make_catalog_tool_context(tool_call_id="call_1"):
    context = CatalogSearchExecutionContext(
        backend=MagicMock(),
        embedder=MagicMock(),
        session_id="test-session",
    )
    arguments = json.dumps({"ranking_query": "fall of the roman empire"})
    return ToolContext(
        context=context,
        tool_name="search_catalog",
        tool_call_id=tool_call_id,
        tool_arguments=arguments,
    )


def make_book_tool_context(tool_call_id="call_1"):
    context = ContentSearchExecutionContext(
        backend=MagicMock(),
        embedder=MagicMock(),
        session_id="test-session",
        edition_id=123,
    )
    arguments = json.dumps({"ranking_query": "chapter one"})
    return ToolContext(
        context=context,
        tool_name="search_book",
        tool_call_id=tool_call_id,
        tool_arguments=arguments,
    )


class TestSearchCatalogToolError:
    async def test_hybrid_search_error_returns_tool_error_prefix(self, mocker):
        mocker.patch(
            "api.assistant.agent.hybrid_search",
            side_effect=RuntimeError("backend unreachable"),
        )
        ctx = make_catalog_tool_context()

        result = await search_catalog.on_invoke_tool(ctx, ctx.tool_arguments)

        assert result.startswith(TOOL_ERROR_PREFIX)

    async def test_embedder_error_returns_tool_error_prefix(self, mocker):
        mocker.patch("api.assistant.agent.hybrid_search")
        ctx = make_catalog_tool_context()
        ctx.context.embedder.embed_query.side_effect = RuntimeError(
            "embedding service down"
        )

        result = await search_catalog.on_invoke_tool(ctx, ctx.tool_arguments)

        assert result.startswith(TOOL_ERROR_PREFIX)

    async def test_invalid_filter_json_error_returns_tool_error_prefix(self, mocker):
        mocker.patch("api.assistant.agent.hybrid_search")
        ctx = make_catalog_tool_context()
        ctx.tool_arguments = json.dumps(
            {"ranking_query": "roman empire", "filters": "not valid filter json"}
        )

        result = await search_catalog.on_invoke_tool(ctx, ctx.tool_arguments)

        assert result.startswith(TOOL_ERROR_PREFIX)


class TestSearchBookToolError:
    async def test_hybrid_search_error_returns_tool_error_prefix(self, mocker):
        mocker.patch(
            "api.assistant.agent.hybrid_search",
            side_effect=RuntimeError("backend unreachable"),
        )
        ctx = make_book_tool_context()

        result = await search_book.on_invoke_tool(ctx, ctx.tool_arguments)

        assert result.startswith(TOOL_ERROR_PREFIX)

    async def test_embedder_error_returns_tool_error_prefix(self, mocker):
        mocker.patch("api.assistant.agent.hybrid_search")
        ctx = make_book_tool_context()
        ctx.context.embedder.embed_query.side_effect = RuntimeError(
            "embedding service down"
        )

        result = await search_book.on_invoke_tool(ctx, ctx.tool_arguments)

        assert result.startswith(TOOL_ERROR_PREFIX)
