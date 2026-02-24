"""Abstract base class for embedders."""

from __future__ import annotations

from abc import ABC, abstractmethod


class Embedder(ABC):
    """Abstract base class for text embedding models.

    Embedders convert text strings into dense vector representations
    suitable for semantic/similarity search.
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

    @abstractmethod
    def embed_one(self, text: str) -> list[float]:
        """Embed a single text string. Returns the embedding vector."""
        ...

    @abstractmethod
    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts in a batch.
        Implementations should handle batching internally if the
        underlying API has batch size limits.
        Returns a list of embedding vectors, one per input text.
        """
        ...
