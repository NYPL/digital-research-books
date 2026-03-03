"""Cohere reranker implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING

import cohere

from vector_indexing.components.rerankers.base import Reranker, RerankResult

if TYPE_CHECKING:
    from vector_indexing.core.types import ChunkDocument


class CohereReranker(Reranker):
    """Reranker using Cohere's rerank API.

    Uses Cohere's reranking models to reorder documents
    based on semantic relevance to a query.
    """

    DEFAULT_MODEL = "rerank-v3.5"

    def __init__(
        self,
        api_key: str | None = None,
        model: str = DEFAULT_MODEL,
    ):
        """Initialize Cohere reranker.

        Args:
            api_key: Cohere API key. If None, uses COHERE_API_KEY env var
            model: model name to use for reranking
        """
        self._model = model
        self._client = cohere.Client(api_key=api_key)

    @property
    def model_name(self) -> str:
        """Return the name of the Cohere reranking model."""
        return f"{self.__class__.__name__}/{self._model}"

    def rerank(
        self,
        query: str,
        documents: list["ChunkDocument"],
        top_k: int | None = None,
    ) -> list[RerankResult]:
        """Rerank chunk documents using Cohere.

        Args:
            query: the search query to rank documents against
            documents: list of chunk documents to rerank
            top_k: optional limit on number of results to return

        Returns:
            List of RerankResult objects sorted by relevance score (descending)
        """
        if not documents:
            return []

        texts = [doc.text for doc in documents]

        response = self._client.rerank(
            model=self._model,
            query=query,
            documents=texts,
            top_n=top_k or len(documents),
        )

        results = []
        for result in response.results:
            idx = result.index
            results.append(
                RerankResult(
                    document=documents[idx],
                    relevance_score=result.relevance_score,
                    index=idx,
                )
            )

        return results
