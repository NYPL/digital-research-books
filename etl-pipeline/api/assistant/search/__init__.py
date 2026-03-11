"""
Hybrid search module for the research assistant.

Provides search execution with result fusion for combining
vector similarity search and BM25 keyword search via Turbopuffer multi-query.
"""

from typing import Optional, TypeAlias

from vector_indexing.core.types import ChunkDocument
from vector_indexing.components.backends.turbopuffer import (
    TurbopufferBackend,
    chunk_from_tpuf_row,
)
from logger import create_log

from .rankfusers import RankFuser, ReciprocalRankFuser


logger = create_log(__name__)


# Type alias for scored search results (ChunkDocument, score)
# Score is typically an RRF fusion score (higher = more relevant)
ScoredHit: TypeAlias = tuple[ChunkDocument, Optional[float]]


def hit_from_row(row) -> ScoredHit:
    """Convert a Turbopuffer row to a ScoredHit tuple."""
    chunk = chunk_from_tpuf_row(row)
    dist = row.model_dump().get("$dist") if hasattr(row, "model_dump") else None
    return (chunk, dist)


def hybrid_search(
    backend: TurbopufferBackend,
    query_vector: list[float],
    ranking_query: str,
    top_k: int = 100,
    filters: list | None = None,
    fuser: RankFuser | None = ReciprocalRankFuser(k=60),
    title_boost: float = 3.0,
    subject_boost: float = 2.0,
) -> list[ScoredHit]:
    """
    Execute hybrid search combining vector similarity and BM25 keyword search.

    Runs both searches in parallel via Turbopuffer multi-query, then fuses
    results using provided fuser.

    Args:
        backend: TurbopufferBackend instance.
        query_vector: Pre-computed embedding vector for semantic search.
        ranking_query: Natural language query for BM25 keyword search.
        top_k: Number of results to retrieve from each search method.
        filters: Optional Turbopuffer filter specification.
        fuser: RankFuser instance (defaults to RRF with k=60).
        title_boost: BM25 boost multiplier for title field.
        subject_boost: BM25 boost multiplier for subject field.

    Returns:
        Fused list of ScoredHits sorted by relevance.
    """

    # Multi-field BM25 with provided boosts
    bm25_rank_by = (
        "Sum",
        (
            ("Product", title_boost, ("title", "BM25", ranking_query)),
            ("Product", subject_boost, ("subject", "BM25", ranking_query)),
            ("text", "BM25", ranking_query),
        ),
    )

    # Build and execute multi-query
    queries = [
        {
            "rank_by": ("vector", "ANN", query_vector),
            "top_k": top_k,
            "filters": filters,
            "exclude_attributes": ["vector"],
        },
        {
            "rank_by": bm25_rank_by,
            "top_k": top_k,
            "filters": filters,
            "exclude_attributes": ["vector"],
        },
    ]
    try:
        multi_result = backend.namespace.multi_query(queries=queries)
    except Exception as e:
        logger.error(f"Multi-query failed: {e}")
        raise

    # Convert each query's results to list[ScoredHit]
    result_lists = [
        [hit_from_row(row) for row in qr.rows] for qr in multi_result.results
    ]
    for i, hits in enumerate(result_lists):
        logger.debug(f"Query {i} returned {len(hits)} hits")

    # return fused results. if a fuser is not provided use RRF with k=60
    fuser = fuser or ReciprocalRankFuser(k=60)
    return fuser.fuse(result_lists)


__all__ = [
    # Types
    "ScoredHit",
    # High-level search
    "hybrid_search",
    # Row conversion
    "hit_from_row",
    # Rank fusion
    "RankFuser",
    "ReciprocalRankFuser",
]
