"""Sentence Transformers embedding implementation."""

from __future__ import annotations

from vector_indexing.components.embedders.base import Embedder


DEFAULT_MODEL = "all-mpnet-base-v2"


class SentenceTransformersEmbedder(Embedder):
    """Sentence Transformers embedding model implementation.

    Takes in:
        model: model name (default: all-mpnet-base-v2)
        truncate_dim: optionally truncate output vectors to this many dimensions.
            Only produces semantically valid results for Matryoshka-trained models
            (e.g. nomic-ai/nomic-embed-text-v1.5, mixedbread-ai/mxbai-embed-large-v1).
            For fixed-size models like all-mpnet-base-v2, dimensions are determined by
            the model architecture and cannot be meaningfully resized.
    """

    def __init__(self, model: str = DEFAULT_MODEL, truncate_dim: int | None = None):
        # import locally to avoid installing if not necessary
        from sentence_transformers import SentenceTransformer

        self._model = model
        self._embedder = SentenceTransformer(
            model, truncate_dim=truncate_dim
        )  # MAYBE: make module level lazily loaded singleton per model
        self._dimensions = self._embedder.get_sentence_embedding_dimension()

    @property
    def dimensions(self) -> int:
        """Return the dimensionality of the embedding vectors."""
        return self._dimensions

    @property
    def model_name(self) -> str:
        """Return the model identifier."""
        return self._model

    # TODO: confirm normalize_embedding=True should be passed (for consistency with hugging face TEI and theoretical reasons)
    def embed_one(self, text: str) -> list[float]:
        return self._embedder.encode(text).tolist()

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return self._embedder.encode(texts).tolist()
