"""SageMaker HF TEI embedding implementation.

Calls a SageMaker endpoint configure with a HuggingFace Text Embeddings Inference
(TEI) based container.
"""

from __future__ import annotations

import asyncio

import boto3
import sagemaker
from sagemaker.huggingface import HuggingFacePredictor
# from sagemaker.predictor import Predictor # requires manual passing of JSONSerializer
# from sagemaker.deserializers import JSONDeserializer
# from sagemaker.serializers import JSONSerializer

from vector_indexing.components.embedders.base import Embedder


class SageMakerEmbedder(Embedder):
    """Embedding model served on a SageMaker HF TEI endpoint.

    Args:
        endpoint_name: Name of the deployed SageMaker endpoint.
        aws_profile: AWS SSO profile to use to authenticate to Sagemaker.
        concurrency: concurrent request in embed_batch()
    """

    def __init__(
        self,
        endpoint_name: str,
        aws_profile: str | None = None,
        concurrency: int = 1,
    ) -> None:
        self._endpoint_name = endpoint_name
        self._aws_profile = aws_profile
        self._concurrency = concurrency
        if self._aws_profile:
            boto3.setup_default_session(profile_name=self._aws_profile)
        self._predictor = HuggingFacePredictor(
            endpoint_name=endpoint_name,
            sagemaker_session=sagemaker.Session(),
        )

    @property
    def dimensions(self) -> int:
        raise NotImplementedError()
        # Q: is there a way to retrieve the dims from the endpoint or endpoint config?

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

    def embed_one(self, text: str) -> list[float]:
        """Embed a single text string."""
        # NOTE: The HF TEI endpoint accepts requests in the form {"inputs": "text"} and returns a list of floats.
        embeddings = self._predictor.predict({"inputs": text})
        return embeddings[0]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts up to `concurrency` in-flight requests."""
        # TODO: look at other sagemaker deployments that support better batch speed and cost like, batch_transform, async inference endpoint
        # TODO: try  {inputs: [<str>, <str>]}
        if self._concurrency <= 1:
            return [self.embed_one(text) for text in texts]
        return asyncio.run(self._embed_batch_async(texts))

    async def _embed_batch_async(self, texts: list[str]) -> list[list[float]]:
        """Async implementation of embed_batch using a semaphore to cap concurrency."""
        # MAYBE: switch to ThreadPoolExecutor
        semaphore = asyncio.Semaphore(self._concurrency)

        async def _embed_one(text: str) -> list[float]:
            async with semaphore:
                result = await asyncio.to_thread(
                    self._predictor.predict, {"inputs": text}
                )
                return result[0]

        return list(await asyncio.gather(*[_embed_one(t) for t in texts]))
