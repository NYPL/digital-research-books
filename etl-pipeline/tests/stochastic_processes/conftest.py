from contextlib import contextmanager
from types import SimpleNamespace
from typing import List
from unittest.mock import patch

import numpy as np
import pytest
from api.assistant.agent import search_catalog
from tests.factories import make_chunk_doc
from vector_indexing.core.types import ChunkDocument


# TODO: see if similar functionality is duplicated elsewhere in teh code/test base
@pytest.fixture
def mock_search_backend(mocker):
    """
    Factory fixture that stubs all external I/O in search_catalog or search_book,
    allowing tests to inject ChunkDocuments as synthetic search results.

    For search_book all chunks should be from the same edition/book.

    Call the returned function with a list of ChunkDocuments to activate the
    mocked backend.
    The fixture stubs:
      - hybrid_search → returns ChunkDocuments as ScoredHits
      - map_editions_and_records → returns synthetic item_ids keyed by barcode
      - get_frbr_data_by_edition → returns SimpleNamespace ORM-like rows
        built from each ChunkDocument's book_metadata (first chunk per edition)
      - Embedder → returns a dummy zero vector from embed_query (via get_index_config())
      - Backend → replaced with a no-op mock (via get_index_config())

    Returns:
        Callable that accepts a list of ChunkDocuments and activates all mocks.
    """
    mock_embedder = mocker.MagicMock()
    mock_embedder.embed_query.return_value = np.zeros(768).tolist()
    mock_backend = mocker.MagicMock()
    mocker.patch(
        "api.assistant.agent.get_index_config",
        return_value={"embedder": mock_embedder, "backend": mock_backend},
    )

    def _setup(chunk_docs: List[ChunkDocument]) -> List[ChunkDocument]:
        scored_hits = [(cd, 0.5) for cd in chunk_docs]
        mocker.patch("api.assistant.agent.hybrid_search", return_value=scored_hits)

        # Assign a synthetic item_id to each unique barcode.
        # results_to_chunk_hits only reads item_id from the mapper, so the
        # value is arbitrary as long as it is non-None.
        unique_barcodes = list(dict.fromkeys(cd.barcode for cd in chunk_docs))
        mapper = {
            barcode: {"item_id": i + 1} for i, barcode in enumerate(unique_barcodes)
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


@contextmanager
def stub_function_tool(tool, return_value: str):
    """
    Generic context manager that stubs any openai agents sdk FunctionTool's
    on_invoke_tool with a fixed return value.

    Usage::

        def test_something(test_session_id):
            with stub_function_tool(search_catalog, "No results found."):
                run_result = await update_chat(...)
    """

    async def _stub(ctx, input) -> str:
        return return_value

    with patch.object(tool, "on_invoke_tool", new=_stub):
        yield
