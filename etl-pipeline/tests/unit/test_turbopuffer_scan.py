"""Unit tests for TurbopufferBackend.scan().

Tests cover:
- Default attribute cursor (id asc) with pagination
- Attribute cursor with explicit rank_by / filters / limit
- NotIn cursor for vector (kNN/ANN) queries
- limit as int vs dict (total + extra keys)
- Return type: (ChunkDocument, Optional[float]) tuples
- Call-site helpers: get_document, scan_all_ids, scan_all_documents
"""

import pytest
from unittest.mock import MagicMock, patch, call

from tests.factories import make_chunk_doc
from vector_indexing.core.types import ChunkDocument
from vector_indexing.components.backends.turbopuffer import (
    TurbopufferBackend,
)


def make_backend() -> TurbopufferBackend:
    """Return a TurbopufferBackend with mocked TP client."""
    with (
        patch("vector_indexing.components.backends.turbopuffer.tpuf") as mock_tpuf,
        patch.dict(
            "os.environ",
            {
                "TURBOPUFFER_API_KEY": "test-key",  # pragma: allowlist secret
                "TURBOPUFFER_REGION": "aws-us-east-1",
            },
        ),
    ):
        mock_client = MagicMock()
        mock_tpuf.Turbopuffer.return_value = mock_client
        mock_client.namespace.return_value = MagicMock()

        backend = TurbopufferBackend(index_name="test-ns")
    return backend


# ---------------------------------------------------------------------------
# Attribute cursor (default rank_by = ("id", "asc"))
# ---------------------------------------------------------------------------


class TestScanLtGtCursor:
    def _make_query_result(self, ids: list[str]) -> list[tuple[ChunkDocument, None]]:
        return [(make_chunk_doc(doc_id=i), None) for i in ids]

    def test_scan_stops_on_short_result(self):
        backend = make_backend()
        page = self._make_query_result(["a_0", "a_1", "a_2"])

        backend.query = MagicMock(return_value=page)

        results = list(backend.scan())

        assert len(results) == 3
        assert all(dist is None for _, dist in results)
        # Only one query call since page < _SCAN_PAGE_SIZE
        assert backend.query.call_count == 1

    def test_cursor_advances_on_second_page(self):
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 2

        page1 = self._make_query_result(["a_0", "a_1"])
        page2 = self._make_query_result(["a_2"])

        backend.query = MagicMock(side_effect=[page1, page2])

        results = list(backend.scan())

        assert len(results) == 3
        # TODO: why not just assert the exact result chunk ids to make sure your get back the chunks in the expected order
        assert backend.query.call_count == 2
        # TODO: This also tests that the scan stops on a short result... so just stat that in comments explicitly and delete the previous test, or is there some reason/benefit to having a spearate test that only tests that?

        # Second call should carry a Gt cursor on "id"
        second_call_kwargs = backend.query.call_args_list[1][1]
        filters = second_call_kwargs["filters"]
        assert filters[0] == "id"
        assert filters[1] == "Gt"

    def test_user_filters_combined_with_cursor(self):
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 2

        page1 = self._make_query_result(["a_0", "a_1"])
        page2 = self._make_query_result(["a_2"])

        backend.query = MagicMock(side_effect=[page1, page2])

        user_filter = ["language", "In", ["en"]]
        list(backend.scan(filters=user_filter))

        # First page: only user filter (no cursor yet)
        first_kwargs = backend.query.call_args_list[0][1]
        assert first_kwargs["filters"] == user_filter

        # Second page: And([user_filter, cursor_filter])
        second_kwargs = backend.query.call_args_list[1][1]
        combined = second_kwargs["filters"]
        assert combined[0] == "And"
        assert user_filter in combined[1]

    # TODO: this feels like a cursor agnostic test
    def test_limit_int_stops_early(self):
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 10

        chunks = self._make_query_result([f"a_{i}" for i in range(10)])
        backend.query = MagicMock(return_value=chunks)

        results = list(backend.scan(limit=3))

        assert len(results) == 3

    def test_limit_dict_total_respected(self):
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 10

        chunks = self._make_query_result([f"a_{i}" for i in range(10)])
        backend.query = MagicMock(return_value=chunks)

        results = list(backend.scan(limit={"total": 4}))

        assert len(results) == 4

    def test_limit_dict_extra_keys_forwarded(self):
        """Extra limit keys (e.g. 'per') are forwarded to query()."""
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 10

        chunks = self._make_query_result(["a_0", "a_1"])
        backend.query = MagicMock(return_value=chunks)

        list(
            backend.scan(limit={"total": 100, "per": {"field": "book_id", "limit": 2}})
        )

        call_kwargs = backend.query.call_args_list[0][1]
        # Should use limit dict form, not top_k
        assert "limit" in call_kwargs
        assert call_kwargs["limit"]["per"] == {"field": "book_id", "limit": 2}
        assert "top_k" not in call_kwargs

    def test_asc_uses_gt_cursor(self):
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 1

        page1 = self._make_query_result(["a_0"])
        page2 = self._make_query_result(["a_1"])
        backend.query = MagicMock(side_effect=[page1, page2, []])
        # Q: why is there an empty 3rd page here, seems unrelated to what is being tested

        list(backend.scan(rank_by=("id", "asc")))

        second_kwargs = backend.query.call_args_list[1][1]
        assert second_kwargs["filters"][1] == "Gt"

    def test_desc_uses_lt_cursor(self):
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 1

        page1 = self._make_query_result(["a_1"])
        page2 = self._make_query_result(["a_0"])
        backend.query = MagicMock(side_effect=[page1, page2, []])
        # Q: why is there an empty 3rd page here, seems unrelated to what is being tested

        list(backend.scan(rank_by=("id", "desc")))

        second_kwargs = backend.query.call_args_list[1][1]
        assert second_kwargs["filters"][1] == "Lt"

    # TODO: order by attribute scans should actually return none as the distance (right?)!
    def test_yields_chunk_and_dist_tuple(self):
        backend = make_backend()
        chunk = make_chunk_doc(doc_id="a_0")
        backend.query = MagicMock(return_value=[(chunk, 0.42)])

        results = list(backend.scan())

        assert len(results) == 1
        returned_chunk, dist = results[0]
        assert returned_chunk is chunk
        assert dist == 0.42

    def test_empty_namespace_returns_nothing(self):
        backend = make_backend()
        backend.query = MagicMock(return_value=[])

        results = list(backend.scan())
        assert results == []


# TODO: I want to group tests by cursor agnostic, and LT/GT cursor and NotIn cursor. propose which tests can be pulled into the cursor agnostic bucket


# ---------------------------------------------------------------------------
# NotIn cursor (vector / BM25 / hybrid rank_by)
# ---------------------------------------------------------------------------


class TestScanNotInCursor:
    def test_knn_uses_notin_cursor(self):
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 2

        page1 = [(make_chunk_doc(doc_id=i), 0.1) for i in ["a_0", "a_1"]]
        page2 = [(make_chunk_doc(doc_id=i), 0.2) for i in ["a_2"]]

        backend.query = MagicMock(side_effect=[page1, page2])

        results = list(backend.scan(rank_by=("vector", "kNN", [0.1, 0.2])))

        assert len(results) == 3
        assert backend.query.call_count == 2

        # Second call must carry a NotIn filter containing first-page IDs
        second_kwargs = backend.query.call_args_list[1][1]
        notin = second_kwargs["filters"]
        assert notin[1] == "NotIn"
        assert set(notin[2]) == {r[0].doc_id for r in results[:2]}

    def test_notin_accumulates_across_pages(self):
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 2

        ids_p1 = ["a_0", "a_1"]
        ids_p2 = ["a_2", "a_3"]
        page1 = [(make_chunk_doc(doc_id=i), 0.1) for i in ids_p1]
        page2 = [(make_chunk_doc(doc_id=i), 0.1) for i in ids_p2]

        backend.query = MagicMock(side_effect=[page1, page2, []])

        list(backend.scan(rank_by=("vector", "kNN", [0.0])))

        third_kwargs = backend.query.call_args_list[2][1]
        notin_ids = set(third_kwargs["filters"][2])
        assert notin_ids == set(ids_p1 + ids_p2)

    def test_user_filters_combined_with_notin(self):
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 2

        page1 = [(make_chunk_doc(doc_id=i), 0.1) for i in ["a_0", "a_1"]]
        page2 = [(make_chunk_doc(doc_id=i), 0.1) for i in ["a_2"]]

        backend.query = MagicMock(side_effect=[page1, page2])

        user_filter = ["language", "In", ["en"]]
        list(backend.scan(rank_by=("vector", "kNN", [0.0]), filters=user_filter))

        # Second page: And([user_filter, NotIn filter])
        second_kwargs = backend.query.call_args_list[1][1]
        combined = second_kwargs["filters"]
        assert combined[0] == "And"
        filter_types = {f[1] for f in combined[1]}
        assert "NotIn" in filter_types

    def test_yields_distances_from_vector_query(self):
        backend = make_backend()
        chunk = _chunk_with_id("a_0")
        backend.query = MagicMock(return_value=[(chunk, 0.77)])

        results = list(backend.scan(rank_by=("vector", "kNN", [0.0])))

        _, dist = results[0]
        assert dist == 0.77

    def test_hybrid_sum_uses_notin_cursor(self):
        """A Sum hybrid rank_by should trigger the NotIn cursor path."""
        backend = make_backend()
        backend._SCAN_PAGE_SIZE = 2

        page = [(_chunk_with_id("a_0"), 0.5)]
        backend.query = MagicMock(return_value=page)

        rank_by = [
            "Sum",
            [[0.7, ["vector", "ANN", [0.1]]], [0.3, ["text", "BM25", "q"]]],
        ]
        list(backend.scan(rank_by=rank_by))

        call_kwargs = backend.query.call_args_list[0][1]
        # First page has no NotIn filter (seen_ids is empty), just top_k
        assert "top_k" in call_kwargs or "limit" in call_kwargs


# ---------------------------------------------------------------------------
# Call-site helpers
# ---------------------------------------------------------------------------


class TestScanHelpers:
    def test_scan_all_ids_yields_doc_ids(self):
        backend = make_backend()
        chunks = [(make_chunk_doc(doc_id=f"a_{i}"), None) for i in range(3)]
        backend.query = MagicMock(return_value=chunks)

        ids = list(backend.scan_all_ids())

        assert len(ids) == 3
        assert all(isinstance(i, str) for i in ids)

    def test_scan_all_documents_yields_chunks(self):
        backend = make_backend()
        chunks = [(make_chunk_doc(doc_id=f"a_{i}"), None) for i in range(3)]
        backend.query = MagicMock(return_value=chunks)

        docs = list(backend.scan_all_documents())

        assert len(docs) == 3
        assert all(isinstance(d, ChunkDocument) for d in docs)

    def test_get_document_returns_chunk_on_hit(self):
        backend = make_backend()
        chunk = make_chunk_doc(doc_id="a_0")
        backend.query = MagicMock(return_value=[(chunk, None)])

        result = backend.get_document("a_0")

        assert result is chunk

    def test_get_document_returns_none_on_miss(self):
        backend = make_backend()
        backend.query = MagicMock(return_value=[])

        result = backend.get_document("missing")

        assert result is None
