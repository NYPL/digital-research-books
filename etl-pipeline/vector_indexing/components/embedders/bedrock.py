"""Amazon Bedrock custom-imported model embedding implementation.

Calls a model imported into Bedrock via the Custom Model Import feature,
backed by a HuggingFace Text Embeddings Inference (TEI) container.
The request/response schema mirrors the TEI standard used by the
SageMaker HF TEI embedder.
"""

from __future__ import annotations

import json

import boto3

from utils.s3 import get_boto3_session_with_assumed_role
from vector_indexing.components.embedders.base import Embedder


# TODO: figure out how to do MRL
# TODO: set dims to NotImplemented unless we can get the info from the endpoint


class BedrockEmbedder(Embedder):
    """Embedding model served via Amazon Bedrock Custom Model Import.

    The ``model_arn`` is the ARN of a successfully imported model, e.g.
    ``arn:aws:bedrock:us-east-1:123456789012:imported-model/xyz``.
    It can be found in the Bedrock console or from the output of the
    deploy_custom_bedrock.py import script.

    Args:
        model_arn: ARN of the imported Bedrock model.
        dimensions: Dimensionality of the output embeddings.
        region: AWS region where the model is hosted.
        aws_profile: AWS SSO profile to apply to the default session. When
            ``assume_role`` is also provided, this profile is used to perform
            the STS AssumeRole call.
        assume_role: ARN of an IAM role to assume for model inference calls.

        .embed_document(), .embed_document_batch(), .embed_query(), and .embed_query_batch()
        inherited from the abstract class raise NotImplementedError.
    """

    def __init__(
        self,
        model_arn: str,
        dimensions: int = None,  # QWEN3_EMBEDDING_8B_DIMS,
        region: str = "us-east-1",
        aws_profile: str | None = None,
        assume_role: str | None = None,
    ) -> None:
        self._model_arn = model_arn
        self._dimensions = dimensions
        if aws_profile:
            boto3.setup_default_session(profile_name=aws_profile)
        if assume_role:
            session = get_boto3_session_with_assumed_role(
                role_arn=assume_role, region_name=region
            )
            self._client = session.client("bedrock-runtime", region_name=region)
        else:
            self._client = boto3.client("bedrock-runtime", region_name=region)

    @property
    def dimensions(self) -> int:
        return self._dimensions

    @property
    def model_name(self) -> str:
        # TODO: how do we get the HF_MODEL_ID from the endpoint (HF TEI /info? same with dims?)
        return self._model_arn

    def embed_one(self, text: str) -> list[float]:
        """Embed a single text string."""
        body = json.dumps({"inputs": text})
        response = self._client.invoke_model(
            modelId=self._model_arn,
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        result = json.loads(response["body"].read())
        return result

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts in a single request."""
        body = json.dumps({"inputs": texts})
        response = self._client.invoke_model(
            modelId=self._model_arn,
            contentType="application/json",
            accept="application/json",
            body=body,
        )
        result = json.loads(response["body"].read())
        return result
