"""Shared factory helpers for ChunkDocument used across tests."""

from contextlib import contextmanager
from typing import List, Optional
from unittest.mock import patch

from vector_indexing.core.types import BookMetadata, ChunkDocument

_UNSET = object()


def make_chunk_doc(
    text: str = "Default text content.",
    title: str = "Default Title",
    edition_id: int = 1,
    book_id: str = "1",
    author: Optional[List[str]] = None,
    subject: Optional[List[str]] = None,
    language: Optional[List[str]] = None,
    publication_date: Optional[str] = "2000-01-01",
    barcode=_UNSET,  # default = "00000000000000"
    chunk_index=_UNSET,  # default = 0
    start_page: int = 1,
    end_page: int = 5,
    doc_id: Optional[str] = None,
) -> ChunkDocument:
    """Factory for ChunkDocument with sensible defaults for use in tests.

    Two usage modes:
    - Pass doc_id to control identity exactly; barcode and chunk_index are
      derived from it (split on "_") and must not be passed explicitly.
    - Omit doc_id and optionally pass barcode/chunk_index; doc_id is then
      generated via ChunkDocument.create() as "barcode_chunkindex". If
      barcode/chunk_index are not passed they default to "00000000000000" and 0
      respectively.
    """
    metadata = BookMetadata(
        edition_id=edition_id,
        title=title,
        author=author if author is not None else [],
        subject=subject if subject is not None else [],
        publication_date=publication_date,
        language=language if language is not None else [],
    )

    if doc_id is not None:
        assert barcode is _UNSET and chunk_index is _UNSET, (
            "Cannot pass barcode or chunk_index when doc_id is specified"
        )
        parts = doc_id.split("_")
        _barcode = parts[0]
        _chunk_index = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        return ChunkDocument(
            doc_id=doc_id,
            text=text,
            barcode=_barcode,
            book_id=book_id,
            chunk_index=_chunk_index,
            start_page=start_page,
            end_page=end_page,
            book_metadata=metadata,
        )

    return ChunkDocument.create(
        barcode=barcode if barcode is not _UNSET else "00000000000000",
        book_id=book_id,
        chunk_index=chunk_index if chunk_index is not _UNSET else 0,
        text=text,
        start_page=start_page,
        end_page=end_page,
        book_metadata=metadata,
    )


@contextmanager
def stub_function_tool(tool, return_value: str):
    """
    Generic context manager that stubs any openai agents sdk FunctionTool's
    on_invoke_tool with a fixed return value.

    Usage::

        def test_something(test_session_id):
            with stub_function_tool(search_catalog, "No results found."):
                run_result = update_chat(...)
    """

    async def _stub(ctx, input) -> str:
        return return_value

    with patch.object(tool, "on_invoke_tool", new=_stub):
        yield
