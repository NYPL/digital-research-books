"""Google Gemini embedding implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from google import genai
from google.genai.errors import ClientError
from ratelimit import limits, sleep_and_retry
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from vector_indexing.components.embedders.base import Embedder

if TYPE_CHECKING:
    from google.genai import Client


# Default model configuration
DEFAULT_MODEL = "gemini-embedding-001"
DEFAULT_DIMS = 768
DEFAULT_BATCH_SIZE = 100
# Rate limit: 20 calls/min with batch size 100 = 2000 embeddings/min
# (3000/min usually breaks the token limit with current chunking leading to
# more backoff attempts and thus lower thruput)
DEFAULT_RATE_LIMIT_CALLS = 20
DEFAULT_RATE_LIMIT_PERIOD = 60  # seconds


def _is_rate_limit_error(exception: BaseException) -> bool:
    """Check if exception is a rate limit error (HTTP 429)."""
    return (
        isinstance(exception, ClientError) and getattr(exception, "code", None) == 429
    )


class GoogleEmbedder(Embedder):
    """Google Gemini embedding model implementation.

    Uses the google-genai client with built-in rate limiting and
    exponential backoff retry for rate limit errors.

    Args:
        model: model name (default: gemini-embedding-001)
        dimensions: output vector dimensions (default: 768)
        batch_size: max elements per API call (default: 100)
        client: optional pre-configured genai.Client instance

    task_type is an invocation-time parameter on embed_one / embed_batch,
    allowing callers to choose any supported task type per call. See:
    https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/task-types

    The semantic helpers embed_document / embed_query / etc. fix the
    appropriate task type automatically.
    """

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        dimensions: int = DEFAULT_DIMS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        client: "Client | None" = None,
    ):
        self._model = model
        self._dimensions = dimensions
        self._batch_size = batch_size
        self._client = client or genai.Client()

    @property
    def dimensions(self) -> int:
        """Return the dimensionality of the embedding vectors."""
        return self._dimensions

    @property
    def model_name(self) -> str:
        """Return the model identifier."""
        return self._model

    @property
    def batch_size(self) -> int:
        """Return the maximum batch size for API calls."""
        return self._batch_size

    def embed_one(
        self, text: str, task_type: str = "RETRIEVAL_DOCUMENT"
    ) -> list[float]:
        result = self._client.models.embed_content(
            model=self._model,
            contents=text,
            config={
                "task_type": task_type,
                "output_dimensionality": self._dimensions,
            },
        )
        return result.embeddings[0].values

    def embed_batch(
        self, texts: list[str], task_type: str = "RETRIEVAL_DOCUMENT"
    ) -> list[list[float]]:
        if not texts:
            return []

        vectors: list[list[float]] = []
        for i in range(0, len(texts), self._batch_size):
            batch = texts[i : i + self._batch_size]
            result = self._call_api(batch, task_type)
            vectors.extend([emb.values for emb in result.embeddings])
        return vectors

    def embed_document(self, text: str) -> list[float]:
        return self.embed_one(text, task_type="RETRIEVAL_DOCUMENT")

    def embed_document_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch(texts, task_type="RETRIEVAL_DOCUMENT")

    def embed_query(self, text: str) -> list[float]:
        return self.embed_one(text, task_type="RETRIEVAL_QUERY")

    def embed_query_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch(texts, task_type="RETRIEVAL_QUERY")

    # ALT FUTURE: use a sleep until end of rate limit duration on
    # token induced rate limits instead of this exponential backoff for those
    @retry(
        stop=stop_after_attempt(7),
        wait=wait_exponential(multiplier=4, max=70),
        retry=retry_if_exception(_is_rate_limit_error),
        before_sleep=lambda retry_state: print(
            f"Rate limit hit, retrying in {retry_state.next_action.sleep:.1f}s "
            f"(attempt {retry_state.attempt_number}): {retry_state.outcome.exception()}"
        ),
    )
    @sleep_and_retry
    @limits(calls=DEFAULT_RATE_LIMIT_CALLS, period=DEFAULT_RATE_LIMIT_PERIOD)
    def _call_api(self, batch: list[str], task_type: str):
        """Make rate-limited API call with retry on rate limit errors."""
        return self._client.models.embed_content(
            model=self._model,
            contents=batch,
            config={
                "task_type": task_type,
                "output_dimensionality": self._dimensions,
            },
        )
