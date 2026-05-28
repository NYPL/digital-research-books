"""Turbopuffer backend implementation.

Simple single-namespace backend analogous to Elasticsearch backend.
Provides a thin wrapper around the turbopuffer SDK.
"""

from __future__ import annotations

import json
import re
import tempfile
from datetime import datetime, timezone
import os
from typing import Any, Dict, TYPE_CHECKING, Any, Dict, Iterator, Optional

import turbopuffer as tpuf
from logger import create_log
from utils.common import require_env

from vector_indexing.components.backends.base import IndexBackend
from vector_indexing.core.config import VECTOR_INDEXING_ROOT
from vector_indexing.core.types import (
    BookMetadata,
    ChunkDocument,
    InsertResult,
    PatchResult,
)
from vector_indexing.core.utils import TimerSet, format_bytes

if TYPE_CHECKING:
    pass

logger = create_log(__name__)


def load_default_schema() -> dict:
    """Load turbopuffer schema from config/schemas/turbopuffer.json."""
    schema_path = VECTOR_INDEXING_ROOT / "config" / "schemas" / "turbopuffer.json"
    with open(schema_path) as f:
        schema = json.load(f)
    schema.pop("$comment", None)
    return schema


def get_default_schema_with_dims(dims: str):
    schema = load_default_schema()
    schema["vector"]["type"] = f"[{dims}]f16"
    return schema


def _is_size_error(e: Exception) -> bool:
    """Check if exception is a payload size error (HTTP 413) or timeout (HTTP 408)."""
    # Check for typed turbopuffer exception with status code
    if isinstance(e, tpuf.APIStatusError):
        status = getattr(e, "status_code", None)
        if status in (413, 408):  # Payload too large OR request timeout
            return True
    # Fallback to string matching for other exception types
    err = str(e).lower()
    return (
        "length limit" in err
        or "too large" in err
        or "413" in err
        or "408" in err
        or "timeout" in err
    )


def _debug_upsert_error(exc: "tpuf.UnprocessableEntityError", rows: list[dict]) -> None:
    """Log the malformed row and dump the full payload to disk for inspection.

    Parses the row index from exc.body (the decoded JSON response dict, e.g.
    {'error': '...upsert_rows[1035]: data did not match...', 'status': 'error'})
    and logs the offending row. Always writes the full payload to a temp file.
    """
    err_str = exc.body.get("error", "") if isinstance(exc.body, dict) else str(exc)

    match = re.search(r"upsert_rows\[(\d+)\]", err_str)
    if match:
        bad_idx = int(match.group(1))
        bad_row = rows[bad_idx] if bad_idx < len(rows) else None
        logger.error(
            f"[DEBUG] Malformed row at upsert_rows[{bad_idx}]:\n"
            f"{json.dumps(bad_row, default=str, indent=2)}"
        )
    else:
        logger.error(f"[DEBUG] Could not parse row index from error: {err_str}")

    dump_path = (
        Path(tempfile.gettempdir())
        / f"tpuf_bad_payload_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    )
    dump_path.write_text(json.dumps({"upsert_rows": rows}, default=str, indent=2))
    logger.error(f"[DEBUG] Full insert payload written to: {dump_path}")


# Conversion utilities between ChunkDocument and turbopuffer row format


def chunk_to_tpuf_row(chunk: ChunkDocument) -> dict:
    """Convert ChunkDocument to turbopuffer row format."""
    return {
        "id": chunk.doc_id,
        "vector": chunk.vector,
        "text": chunk.text,
        "barcode": chunk.barcode,
        "book_id": chunk.book_id,
        "chunk_index": chunk.chunk_index,
        "start_page": chunk.start_page,
        "end_page": chunk.end_page,
        "edition_id": chunk.book_metadata.edition_id,
        "title": chunk.book_metadata.title,
        "author": chunk.book_metadata.author,
        "subject": chunk.book_metadata.subject,
        "publication_date": chunk.book_metadata.publication_date,
        "language": chunk.book_metadata.language,
    }


def chunk_from_tpuf_row(row) -> ChunkDocument:
    """Reconstruct ChunkDocument from turbopuffer row.

    New API: attributes are directly on the row object, not nested.
    Works with both Row objects (from query) and dicts.
    """
    # Handle Row objects by converting to dict
    if hasattr(row, "model_dump"):
        row_dict = row.model_dump()
    elif isinstance(row, dict):
        row_dict = row
    else:
        # Fallback for other object types
        row_dict = {k: getattr(row, k) for k in dir(row) if not k.startswith("_")}

    # Remove special fields
    row_dict.pop("$dist", None)

    book_metadata = BookMetadata(
        edition_id=row_dict.get("edition_id"),
        title=row_dict.get("title"),
        author=row_dict.get("author", []),
        subject=row_dict.get("subject", []),
        publication_date=row_dict.get("publication_date"),
        language=row_dict.get("language", []),
    )

    return ChunkDocument.create(
        barcode=row_dict.get("barcode"),
        book_id=row_dict.get("book_id"),
        chunk_index=row_dict.get("chunk_index"),
        text=row_dict.get("text"),
        start_page=row_dict.get("start_page"),
        end_page=row_dict.get("end_page"),
        book_metadata=book_metadata,
        vector=row_dict.get("vector"),
    )


class TurbopufferBackend(IndexBackend):
    """Turbopuffer backend for read and write operations on a single namespace.

    See TurbopufferBuffer for buffered writes with adaptive batch sizing to take advantage of turbopuffer's cost structure.

    Thin wrapper around turbopuffer SDK. Use ns.query() kwargs directly:

        # ANN search
        results = backend.query(
            rank_by=("vector", "ANN", query_vector),
            filters=["language", "In", ["en"]],
            top_k=10,
        )

        # BM25 full-text search
        results = backend.query(
            rank_by=("text", "BM25", "civil war"),
            top_k=10,
        )

        # Hybrid search
        results = backend.query(
            rank_by=["Sum", [
                [0.7, ["vector", "ANN", query_vector]],
                [0.3, ["text", "BM25", "query"]],
            ]],
            top_k=10,
        )

        # Paginated scan
        for chunk in backend.scan(filters=["language", "In", ["en"]]):
            process(chunk)
    """

    def __init__(
        self,
        index_name: str,
        schema: Dict[str, Any] | None = None,
    ):
        self._index_name = index_name
        self._schema = schema or load_default_schema()

        self._client = tpuf.Turbopuffer(
            api_key=require_env("TURBOPUFFER_API_KEY"),
            region=os.environ.get("TURBOPUFFER_REGION", "aws-us-east-1"),
            timeout=600,  # 10 minute timeout for large uploads
        )
        self._ns = self._client.namespace(index_name)

        self._timers = TimerSet()

    # IndexBackend interface

    @property
    def index_name(self) -> str:
        return self._index_name

    @property
    def timers(self) -> TimerSet:
        """Access internal timing stats."""
        return self._timers

    @property
    def namespace(self) -> tpuf.Namespace:
        """Direct access to underlying turbopuffer namespace."""
        return self._ns

    def exists(self) -> bool:
        """Namespaces always 'exist' in turbopuffer - created on first write."""
        return True

    def create(self, mappings: dict, settings: Optional[dict] = None) -> None:
        """No-op - turbopuffer creates namespaces on demand."""
        logger.info(
            f"Turbopuffer namespace will be created on first write: {self._index_name}"
        )

    def delete(self) -> None:
        """Not implemented - use delete_test_namespace() for test namespaces."""
        raise NotImplementedError(
            "Direct namespace deletion is disabled. "
            "Use delete_test_namespace() for test namespaces."
        )

    # Document Operations

    def get_document(self, doc_id: str) -> Optional[ChunkDocument]:
        """Get a document by ID."""
        try:
            return next(self.scan(filters=["id", "Eq", doc_id], limit=1))
        except StopIteration:
            return None
        except Exception:
            return None

    def delete_document(self, doc_id: str) -> bool:
        """Delete a document by ID."""
        try:
            with self._timers.time("write"):
                self._ns.write(deletes=[doc_id])
            return True
        except Exception:
            return False

    def patch_document(self, doc_id: str, fields: dict) -> bool:
        """Patch a single document by ID.

        Updates only the specified fields. The vector field cannot be patched;
        to update vectors, use get_document + insert instead.

        If the document doesn't exist, the patch is ignored (returns False).

        Args:
            doc_id: Document ID to patch.
            fields: Dict of field names to new values.

        Returns:
            True if the document was patched, False if not found.

        Raises:
            ValueError: If 'vector' is in fields. Use insert() to update vectors.
        """
        if "vector" in fields:
            raise ValueError(
                "Cannot patch 'vector' field; use insert() to replace the document instead."
            )
        try:
            with self._timers.time("write"):
                response = self._ns.write(patch_rows=[{"id": doc_id, **fields}])
            rows_patched = getattr(response, "rows_patched", 0)
            return rows_patched > 0
        except Exception as e:
            logger.error(f"Failed to patch document {doc_id}: {e}")
            return False

    def patch_documents(self, patches: list[dict]) -> PatchResult:
        """Patch multiple documents by ID.

        Each patch dict must include an 'id' field and the fields to update.
        The vector field cannot be patched.

        Patches to non-existent IDs are silently ignored.

        Args:
            patches: List of dicts, each with 'id' and fields to update.
                Example: [{"id": "doc1", "title": "New Title"}, {"id": "doc2", "author": ["New Author"]}]

        Returns:
            PatchResult with counts of patched/skipped documents.

        Raises:
            ValueError: If any patch contains 'vector'. Use insert() instead.
            Exception: Re-raises size errors (HTTP 413/408) so callers (e.g.
                TurbopufferPatchBuffer) can split the batch and retry. Other
                exceptions are caught and reported via PatchResult.failed.
        """
        if not patches:
            return PatchResult()

        for p in patches:
            if "vector" in p:
                raise ValueError(
                    "Cannot patch 'vector' field; use insert() to replace the document instead."
                )

        try:
            with self._timers.time("write"):
                response = self._ns.write(patch_rows=patches)

            rows_patched = getattr(response, "rows_patched", 0)
            skipped = len(patches) - rows_patched

            logger.info(
                f"Turbopuffer patch: rows={len(patches)}, patched={rows_patched}, skipped={skipped}"
            )

            return PatchResult(patched=rows_patched, skipped=skipped)
        except Exception as e:
            # Let size/timeout errors propagate so the buffer can split & retry.
            if _is_size_error(e):
                raise
            logger.error(f"Failed to patch documents: {e}")
            return PatchResult(failed=len(patches), errors=[{"error": str(e)}])

    def patch_by_filter(
        self,
        filters: list,
        patch: dict,
        allow_partial: bool = False,
    ) -> PatchResult:
        """Patch all documents matching a filter.

        The vector field cannot be patched.

        Args:
            filters: Filter expression (same syntax as query filters).
                Example: ["language", "In", ["en", "es"]]
            patch: Dict of fields to update on all matching documents.
                Example: {"status": "archived"}
            allow_partial: If True, allows partial updates when filter matches
                more than 50k documents. When False (default), the operation
                fails if too many documents match.

        Returns:
            PatchResult with count of patched documents and rows_remaining flag.

        Raises:
            ValueError: If patch contains 'vector'. Use insert() instead.
        """
        if "vector" in patch:
            raise ValueError(
                "Cannot patch 'vector' field; use insert() to replace the document instead."
            )
        try:
            write_kwargs = {
                "patch_by_filter": {
                    "filters": filters,
                    "patch": patch,
                }
            }
            if allow_partial:
                write_kwargs["patch_by_filter_allow_partial"] = True

            with self._timers.time("write"):
                response = self._ns.write(**write_kwargs)

            rows_patched = getattr(response, "rows_patched", 0)
            rows_remaining = getattr(response, "rows_remaining", False)

            logger.info(
                f"Turbopuffer patch_by_filter: patched={rows_patched}, rows_remaining={rows_remaining}"
            )

            return PatchResult(patched=rows_patched, rows_remaining=rows_remaining)
        except Exception as e:
            logger.error(f"Failed to patch by filter: {e}")
            return PatchResult(failed=1, errors=[{"error": str(e)}])

    def get_existing_ids(self, candidate_ids: list[str]) -> set[str]:
        """Check which IDs exist in the namespace."""
        if not candidate_ids:
            return set()
        try:
            results = self.query(
                rank_by=("id", "asc"),
                filters=["id", "In", candidate_ids],
                top_k=len(candidate_ids),
            )
            return {chunk.doc_id for chunk, _ in results}
        except Exception as e:
            logger.warning(f"Failed to check existing IDs: {e}")
            return set()

    # Write operations

    def insert(
        self, chunks: list[ChunkDocument], batch_size: Optional[int] = None
    ) -> InsertResult:
        """Insert ChunkDocuments into the namespace.

        Schema is enforced in each write.

        Args:
            chunks: Documents to insert.
            batch_size: Optional batch size. If None, inserts all at once.
        """
        if not chunks:
            return InsertResult()

        total_inserted = 0
        total_written_bytes = 0
        total_estimated_bytes = 0
        batches = (
            [chunks]
            if batch_size is None
            else [chunks[i : i + batch_size] for i in range(0, len(chunks), batch_size)]
        )

        for batch in batches:
            rows = [chunk_to_tpuf_row(c) for c in batch]
            estimated_bytes = sum(c.estimated_bytes for c in batch)
            total_estimated_bytes += estimated_bytes

            write_kwargs = {
                "upsert_rows": rows,
                "distance_metric": "cosine_distance",
                "schema": self._schema,
            }
            # NOTE: vector dims match btw schema and insert rows are enforced with error

            try:
                with self._timers.time("write"):
                    response = self._ns.write(**write_kwargs)
            except tpuf.UnprocessableEntityError as exc:
                # _debug_upsert_error(exc, rows)
                raise

            billing = getattr(response, "billing", None)
            written_bytes = (
                getattr(billing, "billable_logical_bytes_written", 0) if billing else 0
            )
            total_written_bytes += written_bytes

            logger.info(
                f"Turbopuffer write: rows={len(batch)}, "
                f"estimated={format_bytes(estimated_bytes)}, "
                f"written_bytes={format_bytes(written_bytes)}"
            )
            total_inserted += len(batch)

        return InsertResult(
            inserted=total_inserted,
            written_bytes=total_written_bytes,
            estimated_bytes=total_estimated_bytes,
        )

    # Query operations

    def query(self, **kwargs) -> list[tuple[ChunkDocument, Optional[float]]]:
        """Execute a query against turbopuffer.

        Thin wrapper - pass kwargs directly to ns.query().
        Returns list of (ChunkDocument, distance) tuples.

        Args:
            rank_by: Ranking specification (tuple or list)
            top_k: Number of results
            filters: Optional filter specification
            include_attributes: Include attributes in results (default True)
            include_vectors: Include vectors in results
            ... any other ns.query() kwargs
        """
        # Default include_attributes to True if not specified
        if "include_attributes" not in kwargs and "exclude_attributes" not in kwargs:
            kwargs["include_attributes"] = True

        with self._timers.time("query"):
            result = self._ns.query(**kwargs)

        results = []
        for row in result.rows:
            chunk = chunk_from_tpuf_row(row)
            dist = None
            if hasattr(row, "model_dump"):
                row_dict = row.model_dump()
                dist = row_dict.get("$dist")
            results.append((chunk, dist))
        return results

    _SCAN_PAGE_SIZE = 10_000  # Internal batch size for scan operations

    def scan(
        self,
        filters: Optional[list] = None,
        order_by: tuple[str, str] = ("id", "asc"),
        limit: Optional[int] = None,
    ) -> Iterator[ChunkDocument]:
        """Scan/export documents with cursor-based pagination.

        Uses turbopuffer's recommended export pattern with cursor advancement.
        See: https://turbopuffer.com/docs/export

        Args:
            filters: Optional filter specification
            order_by: Tuple of (field, direction). Default ("id", "asc")
            limit: Max docs to return. None = all matching docs.
        """
        rank_field, rank_dir = order_by
        cursor_op = "Gt" if rank_dir == "asc" else "Lt"
        last_value = None
        yielded = 0

        while True:
            # Calculate how many to fetch this page
            page_size = (
                self._SCAN_PAGE_SIZE
                if limit is None
                else min(self._SCAN_PAGE_SIZE, limit - yielded)
            )
            if page_size <= 0:
                break

            # Build filters with cursor
            cursor_filter = (
                [rank_field, cursor_op, last_value] if last_value is not None else None
            )
            combined_filters = (
                ["And", [filters, cursor_filter]]
                if filters and cursor_filter
                else filters or cursor_filter
            )

            kwargs = {
                "rank_by": order_by,
                "top_k": page_size,
            }
            if combined_filters:
                kwargs["filters"] = combined_filters

            results = self.query(**kwargs)

            if not results:
                break

            for chunk, _ in results:
                last_value = getattr(chunk, rank_field, chunk.doc_id)
                yield chunk
                yielded += 1
                if limit is not None and yielded >= limit:
                    return

            # Stop if we got fewer results than requested (no more pages)
            if len(results) < page_size:
                break

    def scan_all_ids(self) -> Iterator[str]:
        """Iterate over all document IDs in the index."""
        for chunk in self.scan():
            yield chunk.doc_id

    def scan_all_documents(self) -> Iterator[ChunkDocument]:
        """Iterate over all documents in the index."""
        yield from self.scan()

    def count(self, query: Optional[dict] = None) -> int:
        """Count documents in the namespace."""
        try:
            info = self._ns.metadata()
            return getattr(info, "approx_row_count", 0) or 0
        except Exception as e:
            logger.warning(f"Failed to count: {e}")
            return 0

    def get_stats(self) -> dict:
        """Get namespace statistics."""
        try:
            info = self._ns.metadata()
            return {
                "index": self._index_name,
                "exists": True,
                "approx_row_count": getattr(info, "approx_row_count", 0) or 0,
                "dimensions": getattr(info, "dimensions", None),
            }
        except Exception as e:
            return {
                "index": self._index_name,
                "error": str(e),
            }


class TurbopufferInsertBuffer:
    """Accumulates chunks and auto-flushes when size limit is reached.

    Adaptive: Starts at 512MB, backs off by 1% on size errors, floor at 100MB.

    Example:
        with TurbopufferBuffer(backend) as buffer:
            for chunk in es.scan_all_documents():
                buffer.add(chunk)
    """

    def __init__(
        self,
        backend: TurbopufferBackend,
        max_bytes: int = 450_000_000,
        min_bytes: int = 100_000_000,
    ):
        self._backend = backend
        self._max_bytes = max_bytes
        self._ceiling = max_bytes  # Never exceed initial max
        self._min_bytes = min_bytes
        self._buffer: list[ChunkDocument] = []
        self._current_bytes = 0
        self.total_inserted = 0
        self.total_written_bytes = 0
        self.total_estimated_bytes = 0

    def add(self, chunk: ChunkDocument) -> InsertResult | None:
        """Add chunk to buffer, flush if size limit exceeded."""
        size = chunk.estimated_bytes
        self._buffer.append(chunk)
        self._current_bytes += size

        if self._current_bytes > self._max_bytes:
            over = self._current_bytes - self._max_bytes
            logger.info(
                f"Buffer {format_bytes(self._current_bytes)} exceeded max by {format_bytes(over)}, flushing {len(self._buffer)} chunks..."
            )
            return self.flush()
        return None

    def flush(self) -> InsertResult:
        """Flush buffer, with adaptive retry on size errors."""
        if not self._buffer:
            return InsertResult()

        batch = self._buffer
        self._buffer = []
        self._current_bytes = 0

        result = self._flush_with_retry(batch)
        self.total_inserted += result.inserted
        self.total_written_bytes += result.written_bytes
        self.total_estimated_bytes += result.estimated_bytes

        # Scale back up by 1% after success (capped at ceiling)
        self._max_bytes = min(self._ceiling, int(self._max_bytes * 1.01))

        return result

    def _flush_with_retry(
        self, batch: list[ChunkDocument], depth: int = 0
    ) -> InsertResult:
        """Try to flush, split batch on size errors.
        NOTE: Really torn on the "split" logic. Would have preferred to move some
        percentage of data back to the buffer and save it for the next batch so that
        we're maximizing cost savings. However, this complicates the interface.
        If an add operation results in a flush, the caller likely expects that the buffer
        is empty if it returns successfully with an InsertResult.
        The adaptive modification of _max_bytes should prevent us from hitting the "split group"
        scenario very often, and it should resolve on the first split attempt.
        If we find that rejection on size is a common issue for us on turbopuffer, we can revisit this decision.
        """
        try:
            return self._backend.insert(batch)
        except Exception as e:
            if depth < 5 and len(batch) > 1 and _is_size_error(e):
                # Back off max_bytes for future flushes
                self._max_bytes = max(self._min_bytes, int(self._max_bytes * 0.95))

                # Split batch in half and flush both parts
                mid = len(batch) // 2
                first_half = batch[:mid]
                second_half = batch[mid:]
                logger.warning(
                    f"Size error at depth {depth}, splitting batch: {len(batch)} -> {len(first_half)} + {len(second_half)}"
                )
                return self._flush_with_retry(
                    first_half, depth + 1
                ) + self._flush_with_retry(second_half, depth + 1)
            raise

    def __enter__(self) -> "TurbopufferInsertBuffer":
        return self

    def __exit__(self, *args) -> None:
        self.flush()


class TurbopufferPatchBuffer:
    """Accumulates patches and auto-flushes when size limit is reached.

    Similar to TurbopufferBuffer but for patch operations.
    Adaptive: Backs off limits on size errors, with retry/split logic.

    Example:
        with TurbopufferPatchBuffer(backend) as buffer:
            for doc_id, new_metadata in updates:
                buffer.add({"id": doc_id, "title": new_metadata.title})
    """

    def __init__(
        self,
        backend: TurbopufferBackend,
        max_bytes: int = 50_000_000,
        min_bytes: int = 5_000_000,
    ):
        self._backend = backend
        self._max_bytes = max_bytes
        self._ceiling = max_bytes
        self._min_bytes = min_bytes
        self._buffer: list[dict] = []
        self._current_bytes = 0
        self.total_patched = 0
        self.total_skipped = 0
        self.total_failed = 0

    @staticmethod
    def _estimate_patch_size(patch: dict) -> int:
        """Estimate serialized size of a patch dict.

        NOTE: Nested dicts are undercounted (treated as 8 bytes). The current
        turbopuffer schema has no nested-object fields. If that changes,
        this estimate will undershoot and flushes will trigger later than expected —
        the buffer's adaptive 413 handling in _flush_with_retry still recovers,
        just less efficiently.
        """
        # Rough estimate: JSON serialization overhead
        size = 50  # Base overhead for dict structure
        for key, value in patch.items():
            size += len(key) + 10  # Key + JSON overhead
            if isinstance(value, str):
                size += len(value.encode("utf-8"))
            elif isinstance(value, list):
                size += (
                    sum(
                        len(v.encode("utf-8")) if isinstance(v, str) else 8
                        for v in value
                    )
                    + len(value) * 5
                )
            else:
                size += 8  # Numbers, booleans, etc.
        return size

    def add(self, patch: dict) -> PatchResult | None:
        """Add a patch to the buffer, flush if limits exceeded.

        Args:
            patch: Dict with 'id' and fields to update.
                Example: {"id": "doc123", "title": "New Title", "author": ["New Author"]}

        Returns:
            PatchResult if buffer was flushed, None otherwise.
        """
        if "id" not in patch:
            raise ValueError("Patch must include 'id' field")

        size = self._estimate_patch_size(patch)
        self._buffer.append(patch)
        self._current_bytes += size

        if self._current_bytes > self._max_bytes:
            logger.info(
                f"Patch buffer {format_bytes(self._current_bytes)} exceeded max, flushing {len(self._buffer)} patches..."
            )
            return self.flush()
        return None

    def flush(self) -> PatchResult:
        """Flush all buffered patches with adaptive retry on size errors."""
        if not self._buffer:
            return PatchResult()

        batch = self._buffer
        self._buffer = []
        self._current_bytes = 0

        result = self._flush_with_retry(batch)
        self.total_patched += result.patched
        self.total_skipped += result.skipped
        self.total_failed += result.failed

        # Scale back up by 1% only on a clean flush (capped at ceiling)
        if result.failed == 0:
            self._max_bytes = min(self._ceiling, int(self._max_bytes * 1.01))

        return result

    def _flush_with_retry(self, batch: list[dict], depth: int = 0) -> PatchResult:
        """Try to flush, split batch on size errors."""
        try:
            return self._backend.patch_documents(batch)
        except Exception as e:
            if depth < 5 and len(batch) > 1 and _is_size_error(e):
                # Back off limit for future flushes
                self._max_bytes = max(self._min_bytes, int(self._max_bytes * 0.95))

                # Split batch in half and flush both parts
                mid = len(batch) // 2
                first_half = batch[:mid]
                second_half = batch[mid:]
                logger.warning(
                    f"Patch size error at depth {depth}, splitting: {len(batch)} -> {len(first_half)} + {len(second_half)}"
                )
                return self._flush_with_retry(
                    first_half, depth + 1
                ) + self._flush_with_retry(second_half, depth + 1)
            raise

    def __enter__(self) -> "TurbopufferPatchBuffer":
        return self

    def __exit__(self, *args) -> None:
        self.flush()


def delete_test_namespace(namespace_name: str) -> None:
    """Delete a turbopuffer namespace. FOR TESTING ONLY.

    This function will only delete namespaces that contain 'test' in the name
    to prevent accidental deletion of production data. If you need to delete a namespace,
    use the tp-scan tool or the API directly.

    Args:
        namespace_name: The name of the namespace to delete. Must contain 'test'.

    Raises:
        ValueError: If namespace_name does not contain 'test'.
    """
    if "test" not in namespace_name.lower():
        raise ValueError(
            f"Refusing to delete namespace '{namespace_name}': "
            "namespace name must contain 'test' to be deleted with this function."
        )

    client = tpuf.Turbopuffer(
        api_key=require_env("TURBOPUFFER_API_KEY"),
        region=os.environ.get("TURBOPUFFER_REGION", "aws-us-east-1"),
        timeout=60,
    )
    ns = client.namespace(namespace_name)

    try:
        ns.delete_all()
        logger.info(f"Deleted test namespace: {namespace_name}")
    except Exception as e:
        logger.error(f"Failed to delete test namespace {namespace_name}: {e}")
        raise
