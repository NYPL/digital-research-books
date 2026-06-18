"""Contract tests for Embedder implementations."""

from unittest.mock import MagicMock, patch

import pytest
from google.genai import types

from vector_indexing.components.embedders.google import (
    Gemini001Embedder,
    Gemini2Embedder,
)
from vector_indexing.components.embedders.openai import OpenAIEmbedder
from vector_indexing.components.embedders.sagemaker import (
    Qwen38BEmbedder,
    Qwen3Embedder,
)


# ---------------------------------------------------------------------------
# Abstract contract bases
# ---------------------------------------------------------------------------


class EmbedderContractTests:
    """Shared contract tests for every Embedder concrete.

    Each concrete subclass must override the `embedder` fixture to supply an
    Embedder instance with all external I/O mocked out.
    """

    @pytest.fixture
    def embedder(self):
        raise NotImplementedError("Subclass must provide an `embedder` fixture")

    def test_embed_document_batch_output_length_matches_input(self, embedder):
        result = embedder.embed_document_batch(["alpha", "beta", "gamma"])
        assert len(result) == 3

    def test_embed_document_batch_returns_empty_list_for_empty_input(self, embedder):
        assert embedder.embed_document_batch([]) == []


class APIEmbedderContractTests(EmbedderContractTests):
    """Additional contract tests for APIEmbedder subclasses.

    Subclasses must also override `unit_embedder` to supply an embedder with
    batch_size=1, used for _make_request call-count and failure-propagation tests.
    """

    @pytest.fixture
    def unit_embedder(self):
        """Return an APIEmbedder with batch_size=1 for call-count tests."""
        pytest.skip("Subclass must provide a unit_embedder fixture (batch_size=1)")

    def test_embed_one_uses_make_request(self, embedder):
        with patch.object(
            embedder, "_make_request", wraps=embedder._make_request
        ) as mock:
            embedder.embed_one("hello")
        mock.assert_called_once()

    def test_embed_batch_call_count_matches_batch_size(self, unit_embedder):
        """batch_size=1 and 3 texts → _make_request called 3 times."""
        with patch.object(
            unit_embedder, "_make_request", wraps=unit_embedder._make_request
        ) as mock:
            unit_embedder.embed_batch(["a", "b", "c"])
        assert mock.call_count == 3

    def test_embed_document_batch_fails_when_single_embedding_fails(
        self, unit_embedder
    ):
        """Mock _make_request as success/error/success; assert the error propagates."""
        original = unit_embedder._make_request
        calls = []

        def make_request_side_effect(texts, **kwargs):
            calls.append(texts)
            if len(calls) == 2:
                raise RuntimeError("simulated API error on call 2")
            return original(texts, **kwargs)

        with patch.object(
            unit_embedder, "_make_request", side_effect=make_request_side_effect
        ):
            with pytest.raises(Exception):
                unit_embedder.embed_document_batch(["a", "b", "c"])


# ---------------------------------------------------------------------------
# BedrockEmbedder
# ---------------------------------------------------------------------------


# class TestBedrockEmbedderContracts(EmbedderContractTests):
#     """
#     TODO: implement fixture — mock boto3 `bedrock-runtime` client.
#     `invoke_model` should return a response whose `body.read()` deserializes
#     to a list of float lists (one per input text).
#     See: vector_indexing/components/embedders/bedrock.py
#     """

#     @pytest.fixture
#     def embedder(self):
#         pytest.skip("TODO: implement BedrockEmbedder fixture")


# ---------------------------------------------------------------------------
# Gemini001Embedder
# ---------------------------------------------------------------------------


class TestGemini001EmbedderContracts(APIEmbedderContractTests):
    """Contract tests for Gemini001Embedder with a mocked genai.Client.

    embed_content is called with a list of strings and must return an object
    whose .embeddings is a list of objects with a .values attribute (list[float]).
    """

    def _make_client(self):
        client = MagicMock()

        def _embed_content(**kwargs):
            contents = kwargs.get("contents", [])
            n = len(contents) if isinstance(contents, list) else 1
            result = MagicMock()
            result.embeddings = [MagicMock(values=[0.1] * 768) for _ in range(n)]
            return result

        client.models.embed_content.side_effect = _embed_content
        return client

    @pytest.fixture
    def embedder(self):
        return Gemini001Embedder(client=self._make_client())

    @pytest.fixture
    def unit_embedder(self):
        return Gemini001Embedder(client=self._make_client(), batch_size=1)


# ---------------------------------------------------------------------------
# Gemini2Embedder
# ---------------------------------------------------------------------------


class TestGemini2EmbedderContracts(APIEmbedderContractTests):
    """Contract + behavioral tests for Gemini2Embedder with a mocked genai.Client.

    The mock client's embed_content returns one 768-d vector per Content object
    in the contents list, mirroring real API behavior.
    """

    @pytest.fixture
    def mock_client(self):
        client = MagicMock()

        def _embed_content(**kwargs):
            contents = kwargs.get("contents", [])
            n = len(contents) if isinstance(contents, list) else 1
            result = MagicMock()
            result.embeddings = [MagicMock(values=[0.1] * 768) for _ in range(n)]
            return result

        client.models.embed_content.side_effect = _embed_content
        return client

    @pytest.fixture
    def embedder(self, mock_client):
        return Gemini2Embedder(client=mock_client)

    @pytest.fixture
    def unit_embedder(self, mock_client):
        return Gemini2Embedder(client=mock_client, batch_size=1)

    # --- Gemini2-specific tests ---

    def test_embed_document_prepends_document_prefix(self, embedder):
        with patch.object(
            embedder, "embed_one", return_value=[0.1] * 768
        ) as mock_embed:
            embedder.embed_document("my text")
        mock_embed.assert_called_once_with("title: none | text: my text")

    def test_embed_query_prepends_query_prefix(self, embedder):
        with patch.object(
            embedder, "embed_one", return_value=[0.1] * 768
        ) as mock_embed:
            embedder.embed_query("my query")
        mock_embed.assert_called_once_with("task: search result | query: my query")

    def test_embed_document_batch_prepends_document_prefix_to_all_texts(self, embedder):
        with patch.object(
            embedder, "embed_batch", return_value=[[0.1] * 768, [0.1] * 768]
        ) as mock_batch:
            embedder.embed_document_batch(["hello", "world"])
        mock_batch.assert_called_once_with(
            ["title: none | text: hello", "title: none | text: world"]
        )

    def test_embed_query_batch_prepends_query_prefix_to_all_texts(self, embedder):
        with patch.object(
            embedder, "embed_batch", return_value=[[0.1] * 768, [0.1] * 768]
        ) as mock_batch:
            embedder.embed_query_batch(["hello", "world"])
        mock_batch.assert_called_once_with(
            [
                "task: search result | query: hello",
                "task: search result | query: world",
            ]
        )

    def test_call_api_wraps_texts_in_content_objects(self, embedder, mock_client):
        embedder.embed_batch(["text1", "text2"])
        contents = mock_client.models.embed_content.call_args.kwargs["contents"]
        assert len(contents) == 2
        assert all(isinstance(c, types.Content) for c in contents)


# ---------------------------------------------------------------------------
# SageMakerEmbedder
# ---------------------------------------------------------------------------


class TestSageMakerEmbedderContracts(APIEmbedderContractTests):
    """Contract tests for SageMaker TEI embedders, exercised via Qwen3Embedder.

    HuggingFacePredictor is patched during construction so no real AWS calls
    are made. predictor.predict returns [[float, ...]] per text in the batch.
    """

    def _make_embedder(self, predict_side_effect=None, batch_size=1):
        with (
            patch("vector_indexing.components.embedders.sagemaker.boto3.Session"),
            patch("vector_indexing.components.embedders.sagemaker.sagemaker.Session"),
            patch(
                "vector_indexing.components.embedders.sagemaker.HuggingFacePredictor"
            ) as MockPredictor,
        ):
            mock_predictor = MagicMock()
            if predict_side_effect is not None:
                mock_predictor.predict.side_effect = predict_side_effect
            else:
                mock_predictor.predict.return_value = [[0.1] * 768]
            MockPredictor.return_value = mock_predictor
            return Qwen3Embedder(
                endpoint_name="test-endpoint", dimensions=768, batch_size=batch_size
            )

    @pytest.fixture
    def embedder(self):
        return self._make_embedder()

    @pytest.fixture
    def unit_embedder(self):
        return self._make_embedder(batch_size=1)


# ---------------------------------------------------------------------------
# OpenAIEmbedder
# ---------------------------------------------------------------------------


class TestOpenAIEmbedderContracts(APIEmbedderContractTests):
    """Contract tests for OpenAIEmbedder with requests.post mocked.

    _make_request posts a batch of texts and expects
    {"data": [{"index": i, "embedding": [float, ...]}, ...]} in return.
    """

    def _make_mock_post(self, side_effect=None):
        mock_post = MagicMock()
        if side_effect is not None:
            mock_post.side_effect = side_effect
        else:

            def _post(*args, **kwargs):
                inputs = kwargs.get("json", {}).get("input", [])
                response = MagicMock()
                response.json.return_value = {
                    "data": [
                        {"index": i, "embedding": [0.1] * 768}
                        for i in range(len(inputs))
                    ]
                }
                return response

            mock_post.side_effect = _post
        return mock_post

    @pytest.fixture
    def embedder(self):
        with patch(
            "vector_indexing.components.embedders.openai.requests.post",
            self._make_mock_post(),
        ):
            yield OpenAIEmbedder(
                base_url="http://localhost:1234",
                model_name="test-model",
                dimensions=768,
            )

    @pytest.fixture
    def unit_embedder(self):
        with patch(
            "vector_indexing.components.embedders.openai.requests.post",
            self._make_mock_post(),
        ):
            yield OpenAIEmbedder(
                base_url="http://localhost:1234",
                model_name="test-model",
                dimensions=768,
                batch_size=1,
            )


# ---------------------------------------------------------------------------
# SentenceTransformersEmbedder
# ---------------------------------------------------------------------------


# class TestSentenceTransformersEmbedderContracts(EmbedderContractTests):
#     """
#     TODO: implement fixture — patch `sentence_transformers.SentenceTransformer`
#     so that `.encode(texts)` returns a numpy array of shape (len(texts), dims).
#     See: vector_indexing/components/embedders/sentence_transformers.py
#     """

#     @pytest.fixture
#     def embedder(self):
#         pytest.skip("TODO: implement SentenceTransformersEmbedder fixture")


# ---------------------------------------------------------------------------
# Qwen38BEmbedder
# ---------------------------------------------------------------------------


class TestQwen38BEmbedderImportWorkaround:
    """Tests for the TEI FP16 NaN workaround. See: https://github.com/huggingface/text-embeddings-inference/issues/845"""

    def test_prepends_space_when_text_starts_with_import(self):
        assert Qwen38BEmbedder._fix_import_prefix("importance") == " importance"

    def test_does_not_alter_text_not_starting_with_import(self):
        assert Qwen38BEmbedder._fix_import_prefix("hello world") == "hello world"


# class TestQwen38BEmbedderContracts(EmbedderContractTests):
#     """
#     TODO: implement fixture — same shape as SageMakerEmbedder fixture but
#     instantiate Qwen38BEmbedder.
#     See: vector_indexing/components/embedders/sagemaker.py
#     """

#     @pytest.fixture
#     def embedder(self):
#         pytest.skip("TODO: implement Qwen38BEmbedder fixture")
