"""
Hybrid search module for the research assistant.

Provides search execution with result fusion for combining
vector similarity search and BM25 keyword search via Turbopuffer multi-query.
"""

import re
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


# Common English stopwords for keyword extraction
STOPWORDS = frozenset(
    [
        "i",
        "me",
        "my",
        "myself",
        "we",
        "our",
        "ours",
        "ourselves",
        "you",
        "your",
        "yours",
        "yourself",
        "yourselves",
        "he",
        "him",
        "his",
        "himself",
        "she",
        "her",
        "hers",
        "herself",
        "it",
        "its",
        "itself",
        "they",
        "them",
        "their",
        "theirs",
        "themselves",
        "what",
        "which",
        "who",
        "whom",
        "this",
        "that",
        "these",
        "those",
        "am",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "have",
        "has",
        "had",
        "having",
        "do",
        "does",
        "did",
        "doing",
        "a",
        "an",
        "the",
        "and",
        "but",
        "if",
        "or",
        "because",
        "as",
        "until",
        "while",
        "of",
        "at",
        "by",
        "for",
        "with",
        "about",
        "against",
        "between",
        "into",
        "through",
        "during",
        "before",
        "after",
        "above",
        "below",
        "to",
        "from",
        "up",
        "down",
        "in",
        "out",
        "on",
        "off",
        "over",
        "under",
        "again",
        "further",
        "then",
        "once",
        "here",
        "there",
        "when",
        "where",
        "why",
        "how",
        "all",
        "any",
        "both",
        "each",
        "few",
        "more",
        "most",
        "other",
        "some",
        "such",
        "no",
        "nor",
        "not",
        "only",
        "own",
        "same",
        "so",
        "than",
        "too",
        "very",
        "s",
        "t",
        "can",
        "will",
        "just",
        "don",
        "should",
        "now",
    ]
)


def to_bm25_query(
    query: str,
    stopwords: frozenset[str] = STOPWORDS,
    min_length: int = 2,
) -> str:
    """
    Convert a natural language query to a BM25 query string.

    Args:
        query: The input query string
        stopwords: Set of words to exclude (defaults to STOPWORDS)
        min_length: Minimum token length to include

    Returns:
        Space-separated keywords for BM25 search
    """
    tokens = re.split(r"[^a-zA-Z0-9]+", query.lower())
    keywords = [
        token
        for token in tokens
        if token and len(token) >= min_length and token not in stopwords
    ]
    bm25_query = " ".join(keywords) if keywords else query
    logger.debug(f"BM25 query: '{query}' -> '{bm25_query}'")
    return bm25_query


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
    bm25_query = to_bm25_query(ranking_query)

    # Multi-field BM25 with provided boosts
    bm25_rank_by = (
        "Sum",
        (
            ("Product", title_boost, ("title", "BM25", bm25_query)),
            ("Product", subject_boost, ("subject", "BM25", bm25_query)),
            ("text", "BM25", bm25_query),
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

    # return fused results
    return fuser.fuse(result_lists)


__all__ = [
    # Types
    "ScoredHit",
    # High-level search
    "hybrid_search",
    # Row conversion
    "hit_from_row",
    # Keyword extraction
    "to_bm25_query",
    "STOPWORDS",
    # Rank fusion
    "RankFuser",
    "ReciprocalRankFuser",
]
