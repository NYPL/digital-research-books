"""OpenAI-compatible embeddings endpoint implementation."""
# see: https://developers.openai.com/api/reference/resources/embeddings/methods/create

from __future__ import annotations

import requests

from vector_indexing.components.embedders.base import APIEmbedder


DEFAULT_BATCH_SIZE = 32


class OpenAIEmbedder(APIEmbedder):
    """Embedding model served by any OpenAI-compatible embeddings endpoint.

    This endpoint is task-agnostic: document and query embeddings are identical,
    so embed_document/embed_query are aliases for embed_one/embed_batch.
    Sub-class this class to implement task-aware embedders for specific models.

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

    def _make_request(self, texts: list[str], **kwargs) -> list[list[float]]:
        """POST a batch of texts to the embeddings endpoint and return vectors in input order."""
        payload = {"model": self.model_name, "input": [t.strip() for t in texts]}
        response = requests.post(
            self.endpoint,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=120,
        )
        response.raise_for_status()
        data = response.json()
        # Sort by index in case the server returns items out of order.
        return [
            item["embedding"] for item in sorted(data["data"], key=lambda x: x["index"])
        ]

    def embed_one(self, text: str) -> list[float]:
        return self._make_request([text])[0]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        vectors: list[list[float]] = []
        for i in range(0, len(texts), self._batch_size):
            vectors.extend(self._make_request(texts[i : i + self._batch_size]))
        return vectors

    def embed_document(self, text: str) -> list[float]:
        return self.embed_one(text)

    def embed_document_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch(texts)

    def embed_query(self, text: str) -> list[float]:
        return self.embed_one(text)

    def embed_query_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch(texts)
