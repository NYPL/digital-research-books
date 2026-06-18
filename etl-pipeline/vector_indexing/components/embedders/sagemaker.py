"""SageMaker HF TEI embedding implementation.

Calls a SageMaker endpoint configure with a HuggingFace Text Embeddings Inference
(TEI) based container.
"""

from __future__ import annotations

import concurrent.futures

import boto3
import sagemaker
from sagemaker.huggingface import HuggingFacePredictor
# from sagemaker.predictor import Predictor # requires manual passing of JSONSerializer
# from sagemaker.deserializers import JSONDeserializer
# from sagemaker.serializers import JSONSerializer

from utils.s3 import get_boto3_session_with_assumed_role
from vector_indexing.components.embedders.base import APIEmbedder


class SageMakerTEIEmbedder(APIEmbedder):
    """Intermediary abstract class for all types of embedding models served on SageMaker HF TEI endpoints.

    Args:
        endpoint_name: Name of the deployed SageMaker endpoint.
        dimensions: Specify dimensions of embedding output. Embeddings will be
            returned normalized regardless. Use to specify output dimensions for
            MRL trained models.
        aws_profile: AWS SSO profile to use to authenticate to SageMaker. When
            ``assume_role`` is also provided, this profile is applied to the
            default session used to perform the STS AssumeRole call.
        assume_role: ARN of an IAM role to assume for model inference calls.
        batch_concurrency: concurrent requests in embed_batch(), ignored by .embed_one()
    """

    def __init__(
        self,
        endpoint_name: str,
        dimensions: int | None = None,
        batch_size: int = 1,
        batch_concurrency: int = 1,
        aws_profile: str | None = None,
        assume_role: str | None = None,
    ) -> None:
        self._endpoint_name = endpoint_name
        self._dimensions = dimensions
        self._aws_profile = aws_profile
        self._batch_size = batch_size
        self._batch_concurrency = batch_concurrency
        boto_session = (
            boto3.Session(profile_name=self._aws_profile)
            if self._aws_profile
            else boto3.Session()
        )
        if assume_role:
            assumed_session = get_boto3_session_with_assumed_role(
                role_arn=assume_role, boto3_session=boto_session
            )
            sm_session = sagemaker.Session(boto_session=assumed_session)
        else:
            sm_session = sagemaker.Session(boto_session=boto_session)
        self._predictor = HuggingFacePredictor(
            endpoint_name=endpoint_name,
            sagemaker_session=sm_session,
        )

    @property
    def dimensions(self) -> int:
        if self._dimensions:
            return self._dimensions
        else:
            raise NotImplementedError()
            # Q: is there a way to retrieve the dims from the endpoint or \
            # endpoint config? other then calling the endpoint and measuring the response?

    @property
    def model_name(self) -> str:
        """Return HF_MODEL_ID (or EndpointName if unavailable)"""
        sm = self._predictor.sagemaker_session.sagemaker_client
        config = sm.describe_endpoint_config(
            EndpointConfigName=self._predictor._get_endpoint_config_name()
        )
        model_name = config["ProductionVariants"][0]["ModelName"]
        model = sm.describe_model(ModelName=model_name)
        return (
            model["PrimaryContainer"]
            .get("Environment", {})
            .get("HF_MODEL_ID", self._endpoint_name)
        )

    def _make_request(self, texts: list[str], **kwargs) -> list[list[float]]:
        """Call the TEI endpoint with a batch of texts and return a list of vectors.

        TEI accepts {"inputs": [str, ...]} and returns [[float, ...], ...].
        L2 normalization is applied by TEI post-truncation by default regardless of `dimensions`.
        """
        prompt_name = kwargs.get("prompt_name")
        extra_args = {}
        if self._dimensions:
            extra_args["dimensions"] = self._dimensions
        if prompt_name:
            extra_args["prompt_name"] = prompt_name
        return self._predictor.predict({"inputs": texts, **extra_args})

    def embed_one(self, text: str, prompt_name: str | None = None) -> list[float]:
        """Embed a single text string."""
        return self._make_request([text], prompt_name=prompt_name)[0]

    def embed_batch(
        self, texts: list[str], prompt_name: str | None = None
    ) -> list[list[float]]:
        """Embed multiple texts, chunked by batch_size with optional concurrency.

        See --max-client-batch-size https://github.com/huggingface/text-embeddings-inference
        TODO: look at other sagemaker deployments for better batch speed/cost: batch_transform, async inference endpoint
        """
        batches = [
            texts[i : i + self._batch_size]
            for i in range(0, len(texts), self._batch_size)
        ]
        if self._batch_concurrency <= 1:
            results: list[list[float]] = []
            for batch in batches:
                results.extend(self._make_request(batch, prompt_name=prompt_name))
            return results
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._batch_concurrency
        ) as executor:
            futures = [
                executor.submit(self._make_request, batch, prompt_name=prompt_name)
                for batch in batches
            ]
            results = []
            for f in futures:
                results.extend(f.result())
            return results


class Qwen3Embedder(SageMakerTEIEmbedder):
    """SageMaker embedder for Qwen3-Embedding models served via HF TEI.

    For details on query vs document prompting for Qwen3-Embedding models,
    see: https://huggingface.co/Qwen/Qwen3-Embedding-8B
    """

    def embed_document(self, text: str) -> list[float]:
        return self.embed_one(text)

    def embed_document_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch(texts)

    def embed_query(self, text: str) -> list[float]:
        return self.embed_one(text, prompt_name="query")

    def embed_query_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch(texts, prompt_name="query")


class Qwen38BEmbedder(Qwen3Embedder):
    """SageMaker embedder for Qwen3-Embedding-8B with a workaround for a TEI bug.

    TEI returns all-NaN vectors for any text whose first token is "import"
    (token ID 474) for Qwen3-Embedding-8B because of a float overflow error
    because TEI allowed dtypes (f16, and f32) are different than the dtype the
    model was trained in (bf16). Prepending a single space
    changes the tokenization and avoids the FP16 overflow.
    See: https://github.com/huggingface/text-embeddings-inference/issues/845
    """

    @staticmethod
    def _fix_import_prefix(text: str) -> str:
        if text.startswith("import"):
            return " " + text
        return text

    def embed_document(self, text: str) -> list[float]:
        return super().embed_document(self._fix_import_prefix(text))

    def embed_document_batch(self, texts: list[str]) -> list[list[float]]:
        return super().embed_document_batch([self._fix_import_prefix(t) for t in texts])

    def embed_query(self, text: str) -> list[float]:
        return super().embed_query(self._fix_import_prefix(text))

    def embed_query_batch(self, texts: list[str]) -> list[list[float]]:
        return super().embed_query_batch([self._fix_import_prefix(t) for t in texts])


class HarrierEmbedder(SageMakerTEIEmbedder):
    """SageMaker embedder for Microsoft Harrier models served via HF TEI.

    For details on query vs document prompting for Harrier models,
    see: https://huggingface.co/microsoft/harrier-oss-v1-27b

    Args:
        query_prompt_name: TEI prompt_name to use for query embeddings.
    """

    def __init__(self, *args, query_prompt_name: str = "web_search_query", **kwargs):
        super().__init__(*args, **kwargs)
        self._query_prompt_name = query_prompt_name

    def embed_document(self, text: str) -> list[float]:
        return self.embed_one(text)

    def embed_document_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch(texts)

    def embed_query(self, text: str) -> list[float]:
        return self.embed_one(text, prompt_name=self._query_prompt_name)

    def embed_query_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch(texts, prompt_name=self._query_prompt_name)


class PplxEmbedder(SageMakerTEIEmbedder):
    """SageMaker embedder for Perplexity pplx-embed-v1 models served via HF TEI.

    pplx-embed-v1 is deliberately trained without instruction prefixes — queries
    and documents are embedded identically. See:
    https://huggingface.co/perplexity-ai/pplx-embed-v1-4b
    """

    def embed_document(self, text: str) -> list[float]:
        return self.embed_one(text)

    def embed_document_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch(texts)

    def embed_query(self, text: str) -> list[float]:
        return self.embed_one(text)

    def embed_query_batch(self, texts: list[str]) -> list[list[float]]:
        return self.embed_batch(texts)
