"""Contract tests for Embedder implementations."""

import pytest


# ---------------------------------------------------------------------------
# Abstract contract base
# ---------------------------------------------------------------------------


class EmbedderContractTests:
    """Shared contract tests for every Embedder concrete.

    Uses an abstract test class pattern: each concrete subclass must override
    the `embedder` fixture to supply an Embedder instance with all external I/O mocked out.
    """

    @pytest.fixture
    def embedder(self):
        raise NotImplementedError("Subclass must provide an `embedder` fixture")

    # TODO: implement — embed_batch returns same number of vectors as input texts
    def test_embed_batch_output_length_matches_input(self, embedder):
        pytest.skip("TODO: implement test")

    # TODO: implement — embed_batch propagates (raises) when any single
    # embedding call fails, rather than silently dropping the item
    def test_embed_batch_fails_when_single_embedding_fails(self, embedder):
        pytest.skip("TODO: implement test")


# ---------------------------------------------------------------------------
# BedrockEmbedder
# ---------------------------------------------------------------------------


class TestBedrockEmbedderContracts(EmbedderContractTests):
    """
    TODO: implement fixture — mock boto3 `bedrock-runtime` client.
    `invoke_model` should return a response whose `body.read()` deserializes
    to a list of float lists (one per input text).
    See: vector_indexing/components/embedders/bedrock.py
    """

    @pytest.fixture
    def embedder(self):
        pytest.skip("TODO: implement BedrockEmbedder fixture")


# ---------------------------------------------------------------------------
# GoogleEmbedder
# ---------------------------------------------------------------------------


class TestGoogleEmbedderContracts(EmbedderContractTests):
    """
    TODO: implement fixture — pass a mock `google.genai.Client` instance.
    `client.models.embed_content` should return an object whose `.embeddings`
    attribute is a list of objects with a `.values` attribute (list[float]).
    See: vector_indexing/components/embedders/google.py
    """

    @pytest.fixture
    def embedder(self):
        pytest.skip("TODO: implement GoogleEmbedder fixture")


# ---------------------------------------------------------------------------
# SageMakerEmbedder
# ---------------------------------------------------------------------------


class TestSageMakerEmbedderContracts(EmbedderContractTests):
    """
    TODO: implement fixture — mock `sagemaker.Session` and
    `HuggingFacePredictor` so that `predictor.predict` returns a list
    containing a single float list (TEI single-text response shape).
    See: vector_indexing/components/embedders/sagemaker.py
    """

    @pytest.fixture
    def embedder(self):
        pytest.skip("TODO: implement SageMakerEmbedder fixture")


# ---------------------------------------------------------------------------
# QwenEmbedder
# ---------------------------------------------------------------------------


class TestQwenEmbedderContracts(EmbedderContractTests):
    """
    TODO: implement fixture — patch `requests.post` to return a mock Response
    whose `.json()` returns `{"data": [{"embedding": [0.1, ...]}]}` for each
    text in the batch.
    See: vector_indexing/components/embedders/qwen.py
    """

    @pytest.fixture
    def embedder(self):
        pytest.skip("TODO: implement QwenEmbedder fixture")


# ---------------------------------------------------------------------------
# SentenceTransformersEmbedder
# ---------------------------------------------------------------------------


class TestSentenceTransformersEmbedderContracts(EmbedderContractTests):
    """
    TODO: implement fixture — patch `sentence_transformers.SentenceTransformer`
    so that `.encode(texts)` returns a numpy array of shape (len(texts), dims).
    See: vector_indexing/components/embedders/sentence_transformers.py
    """

    @pytest.fixture
    def embedder(self):
        pytest.skip("TODO: implement SentenceTransformersEmbedder fixture")
