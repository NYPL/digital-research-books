"""Retrieve cosine-distance retrieval helpers for turbopuffer backends."""

from __future__ import annotations

import time
from typing import Optional

from vector_indexing.components.backends.turbopuffer import TurbopufferBackend


def scan_knn(
    backend: TurbopufferBackend,
    query_vector: list[float],
    log_progress: bool = False,
    log_interval: int = 100_000,
    limit: Optional[int] = None,
) -> list[float]:
    """Retrieve cosine distances for every document in index.

    Uses ``["id", "NotEq", None]`` as a filter that matches all documents,
    satisfying turbopuffer's requirement that kNN queries have a filter while
    still covering the full namespace.

    Args:
        backend: Turbopuffer backend to scan.
        query_vector: Embedded query vector (L2-normalised).
        log_progress: When True, print progress every ``log_interval`` docs and
            a final summary line when the scan completes.
        log_interval: Number of docs between progress log lines.
        limit: Maximum number of results to return.  ``None`` returns all docs.

    Returns:
        List of cosine distances (one per document, exact).
    """
    t0 = time.perf_counter()
    distances: list[float] = []

    for _, dist in backend.scan(
        rank_by=("vector", "kNN", query_vector),
        filters=[
            "id",
            "NotEq",
            None,
        ],  # Filter required for KNN. Matches all documents.
        include_attributes=False,
        limit=limit,
    ):
        if dist is not None:
            distances.append(dist)
        if (
            log_progress
            and log_interval
            and len(distances) % log_interval == 0
            and distances
        ):
            elapsed = time.perf_counter() - t0
            rate = len(distances) / elapsed if elapsed > 0 else 0
            print(f"    {len(distances):,} docs | {elapsed:.1f}s | {rate:,.0f} docs/s")

    if log_progress:
        elapsed = time.perf_counter() - t0
        rate = len(distances) / elapsed if elapsed > 0 else 0
        print(
            f"    Done: {len(distances):,} docs in {elapsed:.1f}s ({rate:,.0f} docs/s)"
        )

    return distances


# BUG Potential Failure Model:
# Early-termination failure mode for large namespaces: ``backend.scan``
# paginates ANN results using a growing ``NotIn`` exclusion list (one entry
# per already-seen doc ID).  Turbopuffer's ANN search explores a limited
# number of relevant clusters on each ANN, so as the exclusion list grows
# the index has fewer candidate clusters and can no longer fill a full page
# from the remaining candidate document — it returns fewer than
# ``_SCAN_PAGE_SIZE`` results, and the pagination loop exits early.
# Solution: page size as small as the smallest possible cluster?
def scan_ann(
    backend: TurbopufferBackend,
    query_vector: list[float],
    limit: Optional[int] = None,
    log_progress: bool = False,
    log_interval: int = 100_000,
) -> list[float]:
    """Retrieve approximately correctly ordered cosine distances for every document in index.

    Args:
        backend: Turbopuffer backend to scan.
        query_vector: Embedded query vector (L2-normalised).
        limit: Maximum number of results to return.  ``None`` returns all docs.
        log_progress: When True, print progress every ``log_interval`` docs and
            a final summary line when the scan completes.
        log_interval: Number of docs between progress log lines.

    Returns:
        List of cosine distances (one per matching chunk).
    """

    t0 = time.perf_counter()
    distances: list[float] = []

    for _, dist in backend.scan(
        rank_by=("vector", "ANN", query_vector),
        limit=limit,
        include_attributes=False,
    ):
        if dist is not None:
            distances.append(dist)
        if (
            log_progress
            and log_interval
            and len(distances) % log_interval == 0
            and distances
        ):
            elapsed = time.perf_counter() - t0
            rate = len(distances) / elapsed if elapsed > 0 else 0
            print(f"    {len(distances):,} docs | {elapsed:.1f}s | {rate:,.0f} docs/s")

    if log_progress:
        elapsed = time.perf_counter() - t0
        rate = len(distances) / elapsed if elapsed > 0 else 0
        print(
            f"    Done: {len(distances):,} docs in {elapsed:.1f}s ({rate:,.0f} docs/s)"
        )

    return distances


# BUG
# Failure Mode:
# Hangs indefinitely when querying large barcodes.... can't quite figure it out
def scan_knn_by_barcode(
    backend: TurbopufferBackend,
    query_vector: list[float],
    log_interval: int = 50,
) -> list[float]:
    """Retrieve cosine distances via exact per-barcode kNN scans.

    Enumerates unique barcodes in index, then issues one kNN query
    per barcode scoped via a filter.

    Args:
        backend: Turbopuffer backend to scan.
        query_vector: Embedded query vector (L2-normalised).
        log_interval: Print progress every N barcodes.  Set to 0 to disable.

    Returns:
        List of cosine distances (one per matching chunk, across all barcodes).
    """
    t0 = time.perf_counter()
    distances: list[float] = []
    n_barcodes = 0

    for chunk, _ in backend.scan(
        rank_by=("barcode", "asc"),
        limit={"per": {"attributes": ["barcode"], "limit": 1}},
        include_attributes=["barcode"],
    ):
        barcode = chunk.barcode
        if not barcode:
            continue

        for _, dist in backend.scan(
            rank_by=("vector", "kNN", query_vector),
            filters=["barcode", "Eq", barcode],
            include_attributes=False,
        ):
            if dist is not None:
                distances.append(dist)

        n_barcodes += 1
        if log_interval and n_barcodes % log_interval == 0:
            elapsed = time.perf_counter() - t0
            print(
                f"    {n_barcodes} barcodes | {len(distances):,} distances | {elapsed:.1f}s"
            )

    elapsed = time.perf_counter() - t0
    print(f"    Done: {len(distances):,} distances in {elapsed:.1f}s")

    return distances
