from google import genai
from google.genai.errors import ClientError
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_google_genai._common import GoogleGenerativeAIError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    wait_fixed,
    retry_if_exception,
)
from ratelimit import limits, sleep_and_retry

from utils.common import batched


GEMINI_EMBEDDING_MODEL = "gemini-embedding-001"
GEMINI_EMBEDDING_DIMS = 768


def _is_langchain_ratelimit_error(exception):
    return isinstance(exception, GoogleGenerativeAIError) and "429" in str(exception)


class LangChainGoogleEmbedder:
    def __init__(self):
        self.embedder = GoogleGenerativeAIEmbeddings(
            model=GEMINI_EMBEDDING_MODEL, request_options={"timeout": 1000}
        )

    def get_embedding(self, query):
        return self.embedder.embed_query(
            text=query,
            task_type="RETRIEVAL_QUERY",
            output_dimensionality=GEMINI_EMBEDDING_DIMS,
        )

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_fixed(60),
        retry=retry_if_exception(_is_langchain_ratelimit_error),
        before_sleep=lambda retry_state: print(
            f"Rate limit hit, retrying in 60s: {retry_state.outcome.exception()}"
        ),
    )
    def get_embeddings(self, texts):
        # random embeddings
        # vectors = [np.random.random(768).tolist() for _ in range(len(texts))]

        # batch embedding
        vectors = self.embedder.embed_documents(
            texts=texts,
            task_type="RETRIEVAL_DOCUMENT",
            output_dimensionality=GEMINI_EMBEDDING_DIMS,
        )

        ## sequential embedding
        # vectors = [
        #     self.embedder.embed_query(
        #         text=text,
        #         task_type='RETRIEVAL_DOCUMENT',
        #         output_dimensionality=GEMINI_EMBEDDING_DIMS,
        #     )
        #     for text in texts
        # ]

        return vectors


def _is_genai_ratelimit_error(exception):
    return (
        isinstance(exception, ClientError) and getattr(exception, "code", None) == 429
    )


# genai_rate_limit_retry = retry(
#     stop=stop_after_attempt(2),
#     wait=wait_fixed(100),
#     retry=retry_if_exception(_is_genai_ratelimit_error),
#     before_sleep=lambda retry_state: print(
#         f"Rate limit hit, retrying in {retry_state.wait}s: {retry_state.outcome.exception()}"
#     ),
# )


class GoogleEmbedder:
    def __init__(self):
        self.client = genai.Client()

    def get_embedding(self, query):
        result = self.client.models.embed_content(
            model=GEMINI_EMBEDDING_MODEL,
            contents=query,
            config=dict(
                task_type="RETRIEVAL_DOCUMENT",
                output_dimensionality=GEMINI_EMBEDDING_DIMS,
            ),
        )
        return result.embeddings[0].values

    # FUTURE: more efficient use a sleep until end of rate limit duration on \
    # token induced rate limits instead of this exponential backoff for those
    @retry(
        stop=stop_after_attempt(7),
        wait=wait_exponential(multiplier=4, max=70),
        retry=retry_if_exception(_is_genai_ratelimit_error),
        before_sleep=lambda retry_state: print(
            f"Rate limit hit, retrying in {retry_state.next_action.sleep}s (attempt {retry_state.attempt_number}): {retry_state.outcome.exception()}"
        ),
    )
    @sleep_and_retry
    # 2000 embeddings/min with the 100 chunk batches sent by get_embeddings() to _call_api()
    # (3000/min usually breaks the token limit with current \
    # chunking leading to more backoff attempts and thus lower thruput)
    @limits(calls=20, period=60)
    def _call_api(self, batch):
        """rate limited and retried API call"""
        return self.client.models.embed_content(
            model=GEMINI_EMBEDDING_MODEL,
            contents=batch,
            config=dict(
                task_type="RETRIEVAL_DOCUMENT",
                output_dimensionality=GEMINI_EMBEDDING_DIMS,
            ),
        )

    def get_embeddings(self, texts):
        vectors = []
        for batch in batched(texts, 100):
            result = self._call_api(batch)
            vectors.extend([emb.values for emb in result.embeddings])
        return vectors


class SentenceTransformersEmbedder:
    def __init__(self):
        # import locally to avoid installing if not necessary
        from sentence_transformers import SentenceTransformer

        # TODO: make this module-level so it is only loaded into memory once (or is that handled implicitly?)
        self.embedder = SentenceTransformer("all-mpnet-base-v2")

    def get_embedding(self, query):
        return self.embedder.encode(query).tolist()

    def get_embeddings(self, texts):
        # random embeddings
        # vectors = [np.random.random(768).tolist() for _ in range(len(texts))]

        vectors = self.embedder.encode(texts).tolist()

        return vectors
