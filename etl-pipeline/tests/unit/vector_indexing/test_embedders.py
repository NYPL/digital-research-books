"""Contract tests for Embedder implementations."""

import pytest

from vector_indexing.components.embedders.sagemaker import Qwen38BEmbedder


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

    # TODO: implement — embed_document_batch returns same number of vectors as input texts
    def test_embed_document_batch_output_length_matches_input(self, embedder):
        pytest.skip("TODO: implement test")

    # TODO: implement — embed_document_batch propagates (raises) when any single
    # embedding call fails, rather than silently dropping the item
    def test_embed_document_batch_fails_when_single_embedding_fails(self, embedder):
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
# OpenAIEmbedder
# ---------------------------------------------------------------------------


class TestOpenAIEmbedderContracts(EmbedderContractTests):
    """
    TODO: implement fixture — patch `requests.post` to return a mock Response
    whose `.json()` returns `{"data": [{"embedding": [0.1, ...]}]}` for each
    text in the batch.
    See: vector_indexing/components/embedders/openai_compat.py
    """

    @pytest.fixture
    def embedder(self):
        pytest.skip("TODO: implement OpenAIEmbedder fixture")


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


# ---------------------------------------------------------------------------
# Qwen38BEmbedder
# ---------------------------------------------------------------------------


class TestQwen38BEmbedderImportWorkaround:
    """Tests for the TEI FP16 NaN workaround. See: https://github.com/huggingface/text-embeddings-inference/issues/845"""

    def test_prepends_space_when_text_starts_with_import(self):
        assert Qwen38BEmbedder._fix_import_prefix("importance") == " importance"

    def test_does_not_alter_text_not_starting_with_import(self):
        assert Qwen38BEmbedder._fix_import_prefix("hello world") == "hello world"


class TestQwen38BEmbedderContracts(EmbedderContractTests):
    """
    TODO: implement fixture — same shape as SageMakerEmbedder fixture but
    instantiate Qwen38BEmbedder.
    See: vector_indexing/components/embedders/sagemaker.py
    """

    @pytest.fixture
    def embedder(self):
        pytest.skip("TODO: implement Qwen38BEmbedder fixture")
