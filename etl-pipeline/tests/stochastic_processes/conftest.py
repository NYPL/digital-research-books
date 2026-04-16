from contextlib import contextmanager
from types import SimpleNamespace
from typing import List, Optional
from unittest.mock import patch

import numpy as np
import pytest

from api.assistant.agent import search_catalog
from vector_indexing.core.types import BookMetadata, ChunkDocument


def make_chunk_doc(
    text: str = "Default text content.",
    title: str = "Default Title",
    edition_id: int = 1,
    book_id: str = "1",
    author: Optional[List[str]] = None,
    subject: Optional[List[str]] = None,
    language: Optional[List[str]] = None,
    publication_date: str = "2000-01-01",
    barcode: str = "00000000000000",
    chunk_index: int = 0,
    start_page: int = 1,
    end_page: int = 5,
) -> ChunkDocument:
    """Factory for ChunkDocument with sensible defaults for use in tests."""
    return ChunkDocument.create(
        barcode=barcode,
        book_id=book_id,
        chunk_index=chunk_index,
        text=text,
        start_page=start_page,
        end_page=end_page,
        book_metadata=BookMetadata(
            edition_id=edition_id,
            title=title,
            author=author if author is not None else [],
            subject=subject if subject is not None else [],
            publication_date=publication_date,
            language=language if language is not None else [],
        ),
    )


@pytest.fixture
def mock_search_backend(mocker):
    """
    Factory fixture that stubs all external I/O in search_catalog or search_book,
    allowing tests to inject ChunkDocuments as synthetic search results.

    For search_book all chunks should be from the same edition/book.

    Call the returned function with a list of ChunkDocuments to activate the
    mocks. The fixture stubs:
      - hybrid_search → returns ChunkDocuments as ScoredHits
      - map_editions_and_records → returns synthetic item_ids keyed by book_id
      - get_frbr_data_by_edition → returns SimpleNamespace ORM-like rows
        built from each ChunkDocument's book_metadata (first chunk per edition)
      - GoogleEmbedder → returns a dummy zero vector from embed_one
      - TurbopufferBackend → replaced with a no-op mock

    Returns:
        Callable that accepts a list of ChunkDocuments and activates all mocks.
    """
    mock_embedder = mocker.MagicMock()
    mock_embedder.embed_one.return_value = np.zeros(768).tolist()
    mocker.patch("api.assistant.agent.GoogleEmbedder", return_value=mock_embedder)
    mocker.patch("api.assistant.agent.TurbopufferBackend")

    def _setup(chunk_docs: List[ChunkDocument]) -> List[ChunkDocument]:
        scored_hits = [(cd, 0.5) for cd in chunk_docs]
        mocker.patch("api.assistant.agent.hybrid_search", return_value=scored_hits)

        # Assign a synthetic item_id to each unique book_id (record_id).
        # results_to_chunk_hits only reads item_id from the mapper, so the
        # value is arbitrary as long as it is non-None.
        unique_book_ids = list(dict.fromkeys(cd.book_id for cd in chunk_docs))
        mapper = {
            int(book_id): {"item_id": i + 1}
            for i, book_id in enumerate(unique_book_ids)
        }
        mocker.patch(
            "api.assistant.agent.map_editions_and_records", return_value=mapper
        )

        # Build ORM-like rows from the first chunk per edition.
        # format_frbr_fields() is the only place these attributes are accessed
        # inside search_catalog, so SimpleNamespace is sufficient.
        editions_by_id: dict = {}
        for cd in chunk_docs:
            eid = cd.book_metadata.edition_id
            if eid not in editions_by_id:
                bm = cd.book_metadata
                editions_by_id[eid] = SimpleNamespace(
                    Work=SimpleNamespace(
                        title=bm.title,
                        authors=[{"name": a} for a in bm.author],
                        subjects=[{"heading": s} for s in bm.subject],
                    ),
                    Edition=SimpleNamespace(
                        id=eid,
                        publication_date=bm.publication_date,
                        publishers=[],  # not in BookMetadata; yields "(Publishers Unavailable)"
                        languages=[{"language": lang} for lang in bm.language],
                    ),
                )

        def _mock_get_frbr(edition_ids):
            return [editions_by_id[eid] for eid in edition_ids if eid in editions_by_id]

        mocker.patch(
            "api.assistant.agent.get_frbr_data_by_edition",
            side_effect=_mock_get_frbr,
        )

        return chunk_docs

    return _setup


# TODO: make this a generic stub_function_tool()
@contextmanager
def stub_search_catalog(return_value: str):
    """
    Context manager that stubs search_catalog.on_invoke_tool with a fixed return value.
    search_catalog is a openai agents sdk FunctionTool.

    Usage::

        def test_something(test_session_id):
            with stub_search_catalog("No results found."):
                run_result = await update_chat(...)
    """

    async def _stub(ctx, input) -> str:
        return return_value

    with patch.object(search_catalog, "on_invoke_tool", new=_stub):
        yield
