"""Local embedding inference via LM Studio"""

# TODO: rename everything to LMStudio because nothing here is specific to Qwen as
# served on LMStudio

from __future__ import annotations

import requests

from vector_indexing.components.embedders.base import Embedder
from vector_indexing.core.config import QwenConfig

# Qwen3-embedding-8b outputs 4096-dimensional vectors
DEFAULT_DIMS = 4096
DEFAULT_BATCH_SIZE = 32


class QwenEmbedder(Embedder):
    """Qwen embedding model implementation.

    Connection details are derived from QwenConfig (defaults to localhost:1234).

    Args:
        qwen_config: QwenConfig instance. If None, uses QwenConfig() defaults.
        dimensions: Output vector dimensions (default: 4096)
        batch_size: Max texts per API call (default: 32)
    """

    def __init__(
        self,
        qwen_config: QwenConfig | None = None,
        dimensions: int = DEFAULT_DIMS,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        self._qwen_config = qwen_config or QwenConfig()
        self._dimensions = dimensions
        self._batch_size = batch_size

    @property
    def dimensions(self) -> int:
        """Return the dimensionality of the embedding vectors."""
        return self._dimensions

    @property
    def model_name(self) -> str:
        """Return the model identifier."""
        return self._qwen_config.model

    @property
    def batch_size(self) -> int:
        """Return the maximum batch size for API calls."""
        return self._batch_size

    @property
    def endpoint(self) -> str:
        """Return the full embeddings endpoint URL."""
        return f"{self._qwen_config.url}/v1/embeddings"

    def embed_one(self, text: str) -> list[float]:
        """Embed a single text string."""
        payload = {
            "model": self.model_name,
            "input": text.strip(),
        }

        response = requests.post(
            self.endpoint,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=60,
        )
        response.raise_for_status()

        data = response.json()
        return data["data"][0]["embedding"]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts in batches."""
        if not texts:
            return []

        vectors: list[list[float]] = []

        for i in range(0, len(texts), self._batch_size):
            batch = [t.strip() for t in texts[i : i + self._batch_size]]

            payload = {
                "model": self.model_name,
                "input": batch,
            }

            response = requests.post(
                self.endpoint,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=120,
            )
            response.raise_for_status()

            data = response.json()
            # Response has an 'index' key, that is the order in which the data is sent.
            # should be returned in order but sort to be safe
            batch_embeddings = sorted(data["data"], key=lambda x: x["index"])
            vectors.extend([item["embedding"] for item in batch_embeddings])

        return vectors
