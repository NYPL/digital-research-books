"""Unit tests for TurbopufferBackend patch operations and TurbopufferPatchBuffer.

Focused on the non-trivial behavior:
- Size-error semantics that drive the buffer's split-and-retry loop
- Buffer's adaptive split path (the main reason this code is non-obvious)
- Vector-field guard (silent server-side error otherwise)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import turbopuffer as tpuf

from vector_indexing.core.config import GlobalConfig
from vector_indexing.components.backends.turbopuffer import (
    TurbopufferBackend,
    TurbopufferPatchBuffer,
)


def _response(rows_patched=0, rows_remaining=False):
    resp = MagicMock()
    resp.rows_patched = rows_patched
    resp.rows_remaining = rows_remaining
    return resp


class _FakeSizeError(tpuf.APIStatusError):
    """Real exception subclass that satisfies isinstance + can be raised."""

    def __init__(self):
        # Bypass parent __init__ which requires a response object
        Exception.__init__(self, "payload too large")
        self.status_code = 413


def _size_error():
    return _FakeSizeError()


@pytest.fixture
def fake_ns():
    return MagicMock()


@pytest.fixture
def backend(fake_ns):
    config = GlobalConfig(turbopuffer_api_key="test-key", turbopuffer_region="local")
    with patch(
        "vector_indexing.components.backends.turbopuffer.tpuf.Turbopuffer"
    ) as mock_client_cls:
        mock_client = MagicMock()
        mock_client.namespace.return_value = fake_ns
        mock_client_cls.return_value = mock_client
        yield TurbopufferBackend(index_name="test-ns", config=config)


class TestPatchDocuments:
    def test_partial_patched_reports_skipped(self, backend, fake_ns):
        """skipped = requested - rows_patched, the contract callers rely on."""
        fake_ns.write.return_value = _response(rows_patched=2)
        result = backend.patch_documents([{"id": f"d{i}"} for i in range(3)])
        assert result.patched == 2
        assert result.skipped == 1
        assert result.failed == 0

    def test_generic_exception_returns_failed_result(self, backend, fake_ns):
        """Non-size errors are captured, not raised, so callers can keep going."""
        fake_ns.write.side_effect = RuntimeError("server exploded")
        result = backend.patch_documents([{"id": "d1"}])
        assert result.failed == 1
        assert result.patched == 0

    def test_size_error_propagates(self, backend, fake_ns):
        """Size errors MUST raise so TurbopufferPatchBuffer can split & retry."""
        fake_ns.write.side_effect = _size_error()
        with pytest.raises(tpuf.APIStatusError):
            backend.patch_documents([{"id": "d1"}])

    def test_rejects_vector_field(self, backend, fake_ns):
        with pytest.raises(ValueError, match="vector"):
            backend.patch_documents([{"id": "d1", "vector": [0.1]}])
        fake_ns.write.assert_not_called()


class TestPatchBuffer:
    def test_add_without_id_raises(self, backend):
        with pytest.raises(ValueError, match="id"):
            TurbopufferPatchBuffer(backend).add({"title": "x"})

    def test_context_manager_flushes_on_exit(self, backend, fake_ns):
        fake_ns.write.return_value = _response(rows_patched=2)
        with TurbopufferPatchBuffer(backend, max_bytes=10_000_000) as buf:
            buf.add({"id": "d1"})
            buf.add({"id": "d2"})
            fake_ns.write.assert_not_called()
        fake_ns.write.assert_called_once()
        assert buf.total_patched == 2

    def test_size_error_triggers_split_and_retry(self, backend, fake_ns):
        """The whole reason _flush_with_retry exists: split on 413, retry halves,
        and back off _max_bytes for future flushes."""
        fake_ns.write.side_effect = [
            _size_error(),
            _response(rows_patched=2),
            _response(rows_patched=2),
        ]
        buf = TurbopufferPatchBuffer(
            backend, max_bytes=100_000_000, min_bytes=1_000_000
        )
        original_max = buf._max_bytes

        result = buf._flush_with_retry([{"id": f"d{i}"} for i in range(4)])

        assert result.patched == 4
        assert fake_ns.write.call_count == 3  # 1 fail + 2 halves
        assert buf._max_bytes < original_max  # adaptive backoff

    def test_no_scale_up_after_failed_flush(self, backend, fake_ns):
        """A failed flush must not increase _max_bytes for the next flush."""
        fake_ns.write.side_effect = RuntimeError("auth failure")
        buf = TurbopufferPatchBuffer(backend, max_bytes=100_000_000)
        original_max = buf._max_bytes
        buf._buffer = [{"id": "d1"}]
        buf._current_bytes = 100
        buf.flush()
        assert buf._max_bytes == original_max
        assert buf.total_failed == 1
