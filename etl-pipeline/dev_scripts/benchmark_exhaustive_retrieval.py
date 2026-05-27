"""Benchmark three approaches for retrieving cosine distances from a turbopuffer index.

Approach 1 (kNN scan):      backend.scan(rank_by=("vector", "kNN", query_vector), limit=TOP_K)
Approach 2 (ANN scan):      backend.scan(rank_by=("vector", "ANN", query_vector), limit=TOP_K)
Approach 3 (per-barcode):   scan unique barcodes via backend.scan(), then KNN query per barcode
"""

from __future__ import annotations

import os
import random
import sys
import time
from pathlib import Path

from dotenv import find_dotenv

PROJ_ROOT = Path(find_dotenv("requirements.txt")).parent
sys.path.insert(0, str(PROJ_ROOT))
os.chdir(PROJ_ROOT)

from utils.load_env import load_env

load_env("config/.env.production")

from vector_indexing.components.backends.turbopuffer import TurbopufferBackend
from vector_indexing.core.config import get_index_config_dict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INDEX_NAME = "vra_test-eval300-harrier_oss_v1_.6b"  # pragma: allowlist secret
TOP_K = 200_000
RANDOM_SEED = 42


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

random.seed(RANDOM_SEED)


def _get_dims() -> int:
    cfg = get_index_config_dict(INDEX_NAME)
    vector_type = cfg.get("schema", {}).get("vector", {}).get("type", "")
    if vector_type.startswith("["):
        return int(vector_type.split("]")[0][1:])
    return 1024


def _random_unit_vector(dims: int) -> list[float]:
    vec = [random.gauss(0, 1) for _ in range(dims)]
    mag = sum(x * x for x in vec) ** 0.5
    return [x / mag for x in vec]


def _fmt_time(s: float) -> str:
    return f"{s:.2f}s" if s < 60 else f"{s / 60:.1f}m {s % 60:.0f}s"


def _summarise(distances: list[float]) -> None:
    if not distances:
        return
    print(
        f"  dist — min={min(distances):.6f}  max={max(distances):.6f}  mean={sum(distances) / len(distances):.6f}"
    )


DIMS = _get_dims()
QUERY_VECTOR = _random_unit_vector(DIMS)
BACKEND = TurbopufferBackend(index_name=INDEX_NAME)

timings: dict[str, tuple[float, int]] = {}

print("=" * 60)
print("Distance Retrieval Benchmark")
print("=" * 60)
print(f"Index:      {INDEX_NAME}")
print(f"Dims:       {DIMS}")
print(f"Top-K cap:  {TOP_K:,}")
print(f"Query vec:  random (seed={RANDOM_SEED}, L2-normalised)")


# ---------------------------------------------------------------------------
# Approach 1 — exhaustive KNN scan
# Paginates with a NotIn cursor, guarantees exact distances for every doc.
# NOTE: kNN requires filters; error.
# ---------------------------------------------------------------------------

# print(f"\n[1] KNN scan  (limit={TOP_K:,})")
# t0 = time.perf_counter()
#
# knn_dists = [dist for _, dist in BACKEND.scan(
#     rank_by=("vector", "kNN", QUERY_VECTOR),
#     limit=TOP_K,
#     include_attributes=["barcode"],
# )]
#
# knn_elapsed = time.perf_counter() - t0
# print(f"  {len(knn_dists):,} docs in {_fmt_time(knn_elapsed)}  ({len(knn_dists)/knn_elapsed:,.0f} docs/s)")
# _summarise([d for d in knn_dists if d is not None])
# timings["KNN scan       "] = (knn_elapsed, len(knn_dists))


# ---------------------------------------------------------------------------
# Approach 2 — approximate ANN scan
# Same pagination strategy as KNN but uses the ANN index — faster, approximate.
# ---------------------------------------------------------------------------

print(f"\n[2] ANN scan  (limit={TOP_K:,})")
t0 = time.perf_counter()

ann_dists = [
    dist
    for _, dist in BACKEND.scan(
        rank_by=("vector", "ANN", QUERY_VECTOR),
        limit=TOP_K,
        include_attributes=["barcode"],
    )
]

ann_elapsed = time.perf_counter() - t0
print(
    f"  {len(ann_dists):,} docs in {_fmt_time(ann_elapsed)}  ({len(ann_dists) / ann_elapsed:,.0f} docs/s)"
)
_summarise([d for d in ann_dists if d is not None])
timings["ANN scan       "] = (ann_elapsed, len(ann_dists))


# ---------------------------------------------------------------------------
# Approach 3 — per-barcode KNN
# Scan unique barcodes (limit.per dedup) and issue a KNN query per barcode
# inside the same loop. Many round trips — expected to be the slowest.
# ---------------------------------------------------------------------------

print(f"\n[3] Per-barcode KNN  (doc cap={TOP_K:,})")
t0 = time.perf_counter()

pb_total_docs = 0
pb_barcodes = 0
pb_dists: list[float] = []

# limit.per keeps 1 row per unique barcode; limit.total is the cursor page size.
for chunk, _ in BACKEND.scan(
    rank_by=("barcode", "asc"),
    limit={"per": {"attributes": ["barcode"], "limit": 1}},
    include_attributes=["barcode"],
):
    barcode = chunk.barcode
    if not barcode:
        continue

    # kNN scan scoped to this barcode's chunks
    for _, dist in BACKEND.scan(
        rank_by=("vector", "kNN", QUERY_VECTOR),
        filters=["barcode", "Eq", barcode],
        limit=TOP_K - pb_total_docs,
        include_attributes=["barcode"],
    ):
        pb_total_docs += 1
        if dist is not None:
            pb_dists.append(dist)
    pb_barcodes += 1
    if pb_barcodes % 50 == 0:
        print(f"  ...{pb_barcodes} barcodes, {pb_total_docs:,} docs")
    if pb_total_docs >= TOP_K:
        break

pb_elapsed = time.perf_counter() - t0
print(
    f"  {pb_total_docs:,} docs across {pb_barcodes} barcodes in {_fmt_time(pb_elapsed)}  ({pb_total_docs / pb_elapsed:,.0f} docs/s)"
)
_summarise(pb_dists)
timings["Per-barcode KNN"] = (pb_elapsed, pb_total_docs)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print("\n" + "=" * 60)
print("Summary")
print("=" * 60)
for label, (elapsed, n_docs) in timings.items():
    rate = n_docs / elapsed if elapsed > 0 else 0
    print(
        f"  {label}: {_fmt_time(elapsed):>8}  {n_docs:>7,} docs  {rate:>7,.0f} docs/s"
    )


# Example Output
# ============================================================
# Distance Retrieval Benchmark
# ============================================================
# Index:      vra_test-eval300-harrier_oss_v1_.6b
# Dims:       1024
# Top-K cap:  200,000
# Query vec:  random (seed=42, L2-normalised)

# [2] ANN scan  (limit=200,000)
#   200,000 docs in 20.50s  (9,755 docs/s)
#   dist — min=0.910061  max=0.996715  mean=0.970482

# [3] Per-barcode KNN  (doc cap=200,000)
#   ...50 barcodes, 88,416 docs
#   ...100 barcodes, 141,939 docs
#   ...150 barcodes, 161,747 docs
#   200,000 docs across 187 barcodes in 15.97s  (12,520 docs/s)
#   dist — min=0.910061  max=1.105824  mean=1.005316

# ============================================================
# Summary
# ============================================================
#   ANN scan       :   20.50s  200,000 docs    9,755 docs/s
#   Per-barcode KNN:   15.97s  200,000 docs   12,520 docs/s
