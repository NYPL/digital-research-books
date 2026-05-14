"""OpenAI-compatible embeddings endpoint implementation."""
# see: https://developers.openai.com/api/reference/resources/embeddings/methods/create

from __future__ import annotations

import requests

from vector_indexing.components.embedders.base import Embedder


DEFAULT_BATCH_SIZE = 32


class OpenAIEmbedder(Embedder):
    """Embedding model served by any OpenAI-compatible embeddings endpoint.

    Args:
        base_url: Base URL of the endpoint (e.g. ``http://localhost:1234``).
            The path ``/v1/embeddings`` is appended automatically.
        model_name: Model identifier to pass in the request payload.
        dimensions: Output vector dimensions. Must match the model's output size.
        batch_size: Max texts per API call (default: 32).
    """

    def __init__(
        self,
        base_url: str,
        model_name: str,
        dimensions: int,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        self._base_url = base_url.rstrip("/")
        self._model_name = model_name
        self._dimensions = dimensions
        self._batch_size = batch_size

    @property
    def dimensions(self) -> int:
        """Return the dimensionality of the embedding vectors."""
        return self._dimensions

    @property
    def model_name(self) -> str:
        """Return the model identifier."""
        return self._model_name

    @property
    def batch_size(self) -> int:
        """Return the maximum batch size for API calls."""
        return self._batch_size

    @property
    def endpoint(self) -> str:
        """Return the full embeddings endpoint URL."""
        return f"{self._base_url}/v1/embeddings"

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
            # Response has an 'index' key indicating the original order.
            # Sort to be safe even if the server returns them out of order.
            batch_embeddings = sorted(data["data"], key=lambda x: x["index"])
            vectors.extend([item["embedding"] for item in batch_embeddings])

        return vectors
