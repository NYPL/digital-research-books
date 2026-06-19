"""Google Gemini embedding implementations."""

from __future__ import annotations
from typing import TYPE_CHECKING

import numpy as np
from google import genai
from google.genai import types
from google.genai.errors import ClientError, ServerError
from ratelimit import limits, sleep_and_retry
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from vector_indexing.components.embedders.base import APIEmbedder
from logger import create_log

if TYPE_CHECKING:
    from google.genai import Client


logger = create_log(__name__)

# Tutorial Docs: # https://ai.google.dev/gemini-api/docs/embeddings
DEFAULT_DIMS = 768
DEFAULT_BATCH_SIZE = 100
# Rate limit: 20 calls/min with batch size 100 = 2000 embeddings/min
# (3000/min usually breaks the token limit with current chunking leading to
# more backoff attempts and thus lower thruput)
# June 2026: new rate limits: reqs/min=20k, toks/min=20M
# DEFAULT_RATE_LIMIT_CALLS = 20
DEFAULT_RATE_LIMIT_CALLS = 15_000
DEFAULT_RATE_LIMIT_PERIOD = 60  # seconds
# TODO: set RPM and TPM and set up _make_request to have @limits derived from the \
# RPM and TPM (for configured tokens per chunk = 750). This will require some factory function

# TODO: add batch_concurrency support, and benchmark to id ideal concurrency.


def _l2_normalize(vector: list[float]) -> list[float]:
    """L2-normalize a vector. Required for gemini-embedding-001 at non-3072 dims."""
    # https://ai.google.dev/gemini-api/docs/embeddings
    embedding_values_np = np.array(vector)
    normed_embedding = embedding_values_np / np.linalg.norm(embedding_values_np)
    return normed_embedding.tolist()


def _is_rate_limit_error(exception: BaseException) -> bool:
    """Check if exception is a rate limit error (HTTP 429)."""
    return (
        isinstance(exception, ClientError) and getattr(exception, "code", None) == 429
    )


def _is_service_unavailable_error(exception: BaseException) -> bool:
    """Check if exception is a service unavailable error (HTTP 503)."""
    # google.genai.errors.ServerError: 503 UNAVAILABLE. {'error': {'code': 503, 'message': 'The service is currently unavailable.', 'status': 'UNAVAILABLE'}}
    return (
        isinstance(exception, ServerError) and getattr(exception, "code", None) == 503
    )


def _should_retry(exception: BaseException) -> bool:
    return _is_rate_limit_error(exception) or _is_service_unavailable_error(exception)


class Gemini001Embedder(APIEmbedder):
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
        model: str = "gemini-embedding-001",
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

    def embed_one(
        self, text: str, task_type: str = "RETRIEVAL_DOCUMENT"
    ) -> list[float]:
        result = self._make_request([text], task_type=task_type)
        return _l2_normalize(result.embeddings[0].values)

    def embed_batch(
        self, texts: list[str], task_type: str = "RETRIEVAL_DOCUMENT"
    ) -> list[list[float]]:
        if not texts:
            return []

        vectors: list[list[float]] = []
        for i in range(0, len(texts), self._batch_size):
            batch = texts[i : i + self._batch_size]
            result = self._make_request(batch, task_type=task_type)
            vectors.extend([_l2_normalize(emb.values) for emb in result.embeddings])
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
        retry=retry_if_exception(_should_retry),
        before_sleep=lambda retry_state: logger.warning(
            f"Retrying in {retry_state.next_action.sleep:.1f}s "
            f"(attempt {retry_state.attempt_number}): {retry_state.outcome.exception()}"
        ),
    )
    @sleep_and_retry
    @limits(calls=DEFAULT_RATE_LIMIT_CALLS, period=DEFAULT_RATE_LIMIT_PERIOD)
    def _make_request(
        self, batch: list[str], task_type: str = "RETRIEVAL_DOCUMENT", **kwargs
    ):
        """Make rate-limited API call with retry on rate limit errors."""
        return self._client.models.embed_content(
            model=self._model,
            contents=batch,
            config={
                "task_type": task_type,
                "output_dimensionality": self._dimensions,
            },
        )


class Gemini2Embedder(APIEmbedder):
    """Google Gemini Embedding 2 model implementation.

    Differences with gemini-001:
    - Task type is specified via instruction prefixes
      in the embedded text:
        - Documents: "title: none | text: {content}"
        - Queries:   "task: search result | query: {content}"
    - For batching, each input is wrapped in a Content object. Passing raw strings
      in a list produces a single aggregated embedding rather than per-item embeddings.
    - For truncated embeddings (MRL below 3072), the API returns
      pre-normalized embeddings.

    Args:
        model: model name (default: gemini-embedding-2)
        dimensions: output vector dimensions — 128-3072, recommend 768/1536/3072 (default: 768)
        batch_size: max Content objects to embed per API call (default: 100)
        client: optional pre-configured genai.Client instance
    """

    def __init__(
        self,
        model: str = "gemini-embedding-2",
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
        return self._dimensions

    @property
    def model_name(self) -> str:
        return self._model

    def embed_one(self, text: str) -> list[float]:
        result = self._make_request([text])
        return result.embeddings[0].values

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        vectors: list[list[float]] = []
        for i in range(0, len(texts), self._batch_size):
            batch = texts[i : i + self._batch_size]
            result = self._make_request(batch)
            vectors.extend([emb.values for emb in result.embeddings])
        return vectors

    def _format_document(self, text: str) -> str:
        return f"title: none | text: {text}"

    def _format_query(self, text: str) -> str:
        return f"task: search result | query: {text}"

    def embed_document(self, text: str) -> list[float]:
        return self.embed_one(self._format_document(text))

    def embed_document_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch([self._format_document(t) for t in texts])

    def embed_query(self, text: str) -> list[float]:
        return self.embed_one(self._format_query(text))

    def embed_query_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch([self._format_query(t) for t in texts])

    # MAYBE: since Content() does work for gemini-001 too (I think), _make_request()
    # could be pulled out to a module function shared by both embedders.
    @retry(
        stop=stop_after_attempt(7),
        wait=wait_exponential(multiplier=4, max=70),
        retry=retry_if_exception(_should_retry),
        before_sleep=lambda retry_state: logger.warning(
            f"Retrying in {retry_state.next_action.sleep:.1f}s "
            f"(attempt {retry_state.attempt_number}): {retry_state.outcome.exception()}"
        ),
    )
    @sleep_and_retry
    @limits(calls=DEFAULT_RATE_LIMIT_CALLS, period=DEFAULT_RATE_LIMIT_PERIOD)
    def _make_request(self, texts: list[str], **kwargs):
        """Make rate-limited API call with retry on rate limit errors."""
        return self._client.models.embed_content(
            model=self._model,
            contents=[
                types.Content(parts=[types.Part.from_text(text=t)]) for t in texts
            ],
            config={"output_dimensionality": self._dimensions},
        )
