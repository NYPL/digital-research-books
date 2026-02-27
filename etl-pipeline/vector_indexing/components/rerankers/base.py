"""Abstract base class for rerankers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vector_indexing.core.types import ChunkDocument


@dataclass
class RerankResult:
    """Result from reranking a document.

    Attributes:
        document: the original chunk document
        relevance_score: score indicating relevance to the query (higher is better)
        index: Original index of the document in the input list
    """

    document: "ChunkDocument"
    relevance_score: float
    index: int


class Reranker(ABC):
    """Abstract base class for document rerankers.

    Rerankers take a query and a list of candidate documents and
    reorder them based on relevance to the query. Used after
    initial retrieval fromn the IndexBackend.
    """

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Return the name/identifier of the reranking model."""
        ...

    @abstractmethod
    def rerank(
        self,
        query: str,
        documents: list["ChunkDocument"],
        top_k: int | None = None,
    ) -> list[RerankResult]:
        """Rerank documents based on relevance to the query.

        Args:
            query: The search query to rank documents against.
            documents: List of chunk documents to rerank.
            top_k: Optional limit on number of results to return.
                   If None, returns all documents reranked.

        Returns:
            List of RerankResult objects sorted by relevance score (descending).
        """
        ...
