"""Google Gemini embedding implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from google import genai
from google.genai import types
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


# Gemini-embedding-001 defaults
DEFAULT_MODEL = "gemini-embedding-001"
DEFAULT_DIMS = 768
DEFAULT_BATCH_SIZE = 100
# Rate limit: 20 calls/min with batch size 100 = 2000 embeddings/min
# (3000/min usually breaks the token limit with current chunking leading to
# more backoff attempts and thus lower thruput)
DEFAULT_RATE_LIMIT_CALLS = 20
DEFAULT_RATE_LIMIT_PERIOD = 60  # seconds

# Gemini-embedding-2 defaults
DEFAULT_MODEL_2 = "gemini-embedding-2"
DEFAULT_DIMS_2 = 768
DEFAULT_BATCH_SIZE_2 = 100


def _is_rate_limit_error(exception: BaseException) -> bool:
    """Check if exception is a rate limit error (HTTP 429)."""
    return (
        isinstance(exception, ClientError) and getattr(exception, "code", None) == 429
    )


class Gemini001Embedder(Embedder):
    """Google Gemini embedding-001 model implementation.

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
            contents=[
                types.Content(parts=[types.Part.from_text(text=t)]) for t in batch
            ],
            config={
                "task_type": task_type,
                "output_dimensionality": self._dimensions,
            },
        )


class Gemini2Embedder(Embedder):
    """Google Gemini Embedding 2 model implementation.

    Unlike gemini-embedding-001, this model does not support task_type as a
    parameter. Task context is specified via instruction prefixes in the input text:
      - Documents: "title: none | text: {content}"
      - Queries:   "task: search result | query: {content}"

    For batching, each input is wrapped in a Content object. Passing raw strings
    in a list produces a single aggregated embedding rather than per-item embeddings.

    Args:
        model: model name (default: gemini-embedding-2)
        dimensions: output vector dimensions — 128-3072, recommend 768/1536/3072 (default: 3072)
        batch_size: max Content objects per API call (default: 100)
        client: optional pre-configured genai.Client instance
    """

    def __init__(
        self,
        model: str = DEFAULT_MODEL_2,
        dimensions: int = DEFAULT_DIMS_2,
        batch_size: int = DEFAULT_BATCH_SIZE_2,
        client: "Client | None" = None,
    ):
        self._model = model
        self._dimensions = dimensions
        self._batch_size = batch_size
        self._client = client or genai.Client()

    @property
    def dimensions(self) -> int:
        return self._dimensions

    @property
    def model_name(self) -> str:
        return self._model

    @property
    def batch_size(self) -> int:
        return self._batch_size

    def embed_one(self, text: str) -> list[float]:
        result = self._call_api([text])
        return result.embeddings[0].values

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        vectors: list[list[float]] = []
        for i in range(0, len(texts), self._batch_size):
            batch = texts[i : i + self._batch_size]
            result = self._call_api(batch)
            vectors.extend([emb.values for emb in result.embeddings])
        return vectors

    def embed_document(self, text: str) -> list[float]:
        return self.embed_one(f"title: none | text: {text}")

    def embed_document_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch([f"title: none | text: {t}" for t in texts])

    def embed_query(self, text: str) -> list[float]:
        return self.embed_one(f"task: search result | query: {text}")

    def embed_query_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch([f"task: search result | query: {t}" for t in texts])

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
    def _call_api(self, texts: list[str]):
        """Make rate-limited API call with retry on rate limit errors."""
        return self._client.models.embed_content(
            model=self._model,
            contents=[
                types.Content(parts=[types.Part.from_text(text=t)]) for t in texts
            ],
            config={"output_dimensionality": self._dimensions},
        )
