import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from agents.tool_context import ToolContext

from api.assistant.agent import (
    CatalogSearchExecutionContext,
    ContentSearchExecutionContext,
)
from api.assistant.types import CatalogSearchResult


def make_catalog_search_result(
    edition_id=1,
    barcode="00000000000001",
    title="A Tale of Two Cities",
    authors=("Charles Dickens",),
    subjects=("Fiction",),
    languages=("English",),
    publishers=("Chapman and Hall",),
    pub_date="1859",
    chunk_texts=("It was the best of times.",),
):
    """Factory for CatalogSearchResult with sensible defaults for use in tests."""
    orm_work = SimpleNamespace(
        title=title,
        authors=[{"name": a} for a in authors],
        subjects=[{"heading": s} for s in subjects],
    )
    orm_edition = SimpleNamespace(
        publication_date=pub_date,
        languages=[{"language": lang} for lang in languages],
        publishers=[{"name": p} for p in publishers],
    )
    chunk_hits = [
        {"text": t, "item_id": i + 1, "start_page": i + 1, "end_page": i + 1}
        for i, t in enumerate(chunk_texts)
    ]
    return CatalogSearchResult(
        edition_id=edition_id,
        barcode=barcode,
        orm_work=orm_work,
        orm_edition=orm_edition,
        agg_score=0.9,
        chunk_hits=chunk_hits,
    )


def make_search_catalog_tool_context(
    tool_call_id="call_1",
    ranking_query="fall of the roman empire",
    tool_arguments=None,
    backend=None,
    embedder=None,
):
    """Factory for a ToolContext wrapping CatalogSearchExecutionContext, for
    driving search_catalog.on_invoke_tool() directly in tests."""
    context = CatalogSearchExecutionContext(
        backend=backend if backend is not None else MagicMock(),
        embedder=embedder if embedder is not None else MagicMock(),
    )
    arguments = tool_arguments or json.dumps({"ranking_query": ranking_query})
    return ToolContext(
        context=context,
        tool_name="search_catalog",
        tool_call_id=tool_call_id,
        tool_arguments=arguments,
    )


def make_search_book_tool_context(
    tool_call_id="call_1",
    edition_id=123,
    ranking_query="chapter one",
    tool_arguments=None,
    frbr_fields=None,
    backend=None,
    embedder=None,
):
    """Factory for a ToolContext wrapping ContentSearchExecutionContext, for
    driving search_book.on_invoke_tool() directly in tests."""
    context = ContentSearchExecutionContext(
        backend=backend if backend is not None else MagicMock(),
        embedder=embedder if embedder is not None else MagicMock(),
        edition_id=edition_id,
        frbr_fields=frbr_fields if frbr_fields is not None else {},
    )
    arguments = tool_arguments or json.dumps({"ranking_query": ranking_query})
    return ToolContext(
        context=context,
        tool_name="search_book",
        tool_call_id=tool_call_id,
        tool_arguments=arguments,
    )
