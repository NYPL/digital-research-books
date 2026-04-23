"""SageMaker HF TEI embedding implementation.

Calls a SageMaker endpoint configure with a HuggingFace Text Embeddings Inference
(TEI) based container.
"""

from __future__ import annotations

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
        dimensions: Dimensionality of the output embeddings.
    """

    def __init__(
        self,
        endpoint_name: str,
    ) -> None:
        self._endpoint_name = endpoint_name
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
        response = self._predictor.predict({"inputs": text})
        return response

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts, one request per text (endpoint only accepts a single input)."""
        # TODO: look at other sagemaker deployments that support better batch speed and cost
        # TODO: try I think a list of texts is actually allowed via this HF TEI config
        return [self.embed_one(text) for text in texts]
