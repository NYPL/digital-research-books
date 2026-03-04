"""
Rank fusion strategies for combining multiple search result lists.
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Optional, TypeAlias

from vector_indexing.core.types import ChunkDocument
from logger import create_log


logger = create_log(__name__)


# Type alias for scored search results (ChunkDocument, score)
ScoredHit: TypeAlias = tuple[ChunkDocument, Optional[float]]


class RankFuser(ABC):
    """Abstract base class for fusing multiple ranked result lists."""

    @abstractmethod
    def fuse(self, result_lists: list[list[ScoredHit]]) -> list[ScoredHit]:
        """
        Combine multiple ranked result lists into a single fused ranking.

        Args:
            result_lists: List of ranked result lists, each containing ScoredHits

        Returns:
            Fused and re-ranked list of ScoredHits
        """
        ...


class ReciprocalRankFuser(RankFuser):
    """
    Reciprocal Rank Fusion (RRF) for combining multiple rankings.

    RRF score for a document d across multiple rankings is:
        RRF(d) = sum(1 / (k + rank_i(d))) for all rankings i

    where k is a constant (typically 60) and rank_i(d) is the 1-based rank
    of document d in i-th ranking (or infinity if not present).

    RRF is effective when raw scores from different retrieval methods are
    not directly comparable (e.g., cosine similarity vs BM25 scores).
    """

    def __init__(self, k: int = 60):
        """
        Args:
            k: RRF constant. Higher values reduce the impact of high rankings. Default 60
        """
        self.k = k

    def fuse(self, result_lists: list[list[ScoredHit]]) -> list[ScoredHit]:
        """
        Fuse multiple result lists using Reciprocal Rank Fusion.

        Args:
            result_lists: List of ranked result lists

        Returns:
            Fused results sorted by RRF score (descending)
        """
        if not result_lists:
            return []

        rrf_scores: dict[str, float] = defaultdict(float)
        hits_by_id: dict[str, ScoredHit] = {}

        for result_list in result_lists:
            for rank, hit in enumerate(result_list, start=1):
                chunk, _ = hit
                doc_id = chunk.doc_id

                # Accumulate RRF scores by document ID
                rrf_scores[doc_id] = rrf_scores.get(doc_id, 0) + 1.0 / (self.k + rank)

                # Store the hit (allow overwrite)
                hits_by_id[doc_id] = hit

        # Sort descending by RRF score
        sorted_ids = sorted(
            rrf_scores.keys(), key=lambda x: rrf_scores[x], reverse=True
        )

        # Fuse results
        fused_results: list[ScoredHit] = []
        for doc_id in sorted_ids:
            chunk, _ = hits_by_id[doc_id]
            rrf_score = rrf_scores[doc_id]
            fused_results.append((chunk, rrf_score))

        logger.debug(
            f"RRF fused {sum(len(r) for r in result_lists)} hits from "
            f"{len(result_lists)} lists into {len(fused_results)} unique results"
        )

        return fused_results
