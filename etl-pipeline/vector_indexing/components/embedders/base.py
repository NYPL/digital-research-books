"""Abstract base class for embedders."""

from __future__ import annotations

from abc import ABC, abstractmethod


class Embedder(ABC):
    """Abstract base class for text embedding models.

    Embedders convert text strings into dense vector representations
    suitable for semantic/simililaritying search.
    """

    @property
    @abstractmethod
    def dimensions(self) -> int:
        """Return the dimensionality of the embedding vectors."""
        ...

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Return the name/identifier of the embedding model."""
        ...

    def embed_document(self, text: str) -> list[float]:
        """Embed a single document (indexed document for retrieval)."""
        raise NotImplementedError

    def embed_document_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of documents (indexed documents for retrieval).

        Implementations should handle batching internally if the underlying
        API has batch size limits, and should fail if any single embedding fails.
        """
        raise NotImplementedError

    def embed_query(self, text: str) -> list[float]:
        """Embed a single query (search-time input)."""
        raise NotImplementedError

    def embed_query_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of queries (search-time inputs).

        Implementations should handle batching internally if the underlying
        API has batch size limits, and should fail if any single embedding fails.
        """
        raise NotImplementedError

    # NOTE: the usage pattern in Pipeline.index_books(), requires batch failure
    # if any single embedding fails


class APIEmbedder(Embedder, ABC):
    """Abstract base for API-backed embedders.

    Subclasses must implement ``_make_request`` which performs a single batched
    API call. ``embed_one`` and ``embed_batch`` should route through it so that
    failures surface consistently.
    """

    @abstractmethod
    def _make_request(self, texts: list[str], **kwargs):
        """Make a single batched API call and return raw results."""
        ...
