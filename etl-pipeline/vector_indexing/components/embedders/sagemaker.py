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
from vector_indexing.components.embedders.base import Embedder


class SageMakerEmbedder(Embedder):
    """Embedding model served on a SageMaker HF TEI endpoint.

    Args:
        endpoint_name: Name of the deployed SageMaker endpoint.
        dimensions: Specify dimensions of embedding output. Embeddings will be
            returned normalized regardless. Use to specify output dimensions for
            MRL trained models.
        aws_profile: AWS SSO profile to use to authenticate to SageMaker. When
            ``assume_role`` is also provided, this profile is applied to the
            default session used to perform the STS AssumeRole call.
        assume_role: ARN of an IAM role to assume for model inference calls.
        concurrency: concurrent request in embed_batch()
    """

    def __init__(
        self,
        endpoint_name: str,
        dimensions: int | None = None,
        concurrency: int = 1,
        aws_profile: str | None = None,
        assume_role: str | None = None,
    ) -> None:
        self._endpoint_name = endpoint_name
        self._dimensions = dimensions
        self._aws_profile = aws_profile
        self._concurrency = concurrency
        if self._aws_profile:
            boto3.setup_default_session(profile_name=self._aws_profile)
        if assume_role:
            session = get_boto3_session_with_assumed_role(role_arn=assume_role)
            sm_session = sagemaker.Session(boto_session=session)
        else:
            sm_session = sagemaker.Session()
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

    def embed_one(self, text: str) -> list[float]:
        """Embed a single text string."""
        extra_args = {}
        if self._dimensions:
            extra_args["dimensions"] = self._dimensions

        # NOTE: The HF TEI endpoint accepts requests in the form {"inputs": "text"} and returns a list of floats.
        # NOTE: L2 normalization is applied by default regardless of `dimensions`
        embeddings = self._predictor.predict({"inputs": text, **extra_args})
        return embeddings[0]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts up to `concurrency` in-flight requests."""
        # TODO: look at other sagemaker deployments that support better batch speed and cost like, batch_transform, async inference endpoint
        # TODO: try  {inputs: [<str>, <str>]}. see --max-client-batch-size https://github.com/huggingface/text-embeddings-inference
        if self._concurrency <= 1:
            return [self.embed_one(text) for text in texts]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._concurrency
        ) as executor:
            return list(executor.map(self.embed_one, texts))
