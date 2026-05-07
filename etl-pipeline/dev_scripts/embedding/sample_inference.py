"""Sample SageMaker embedding inference snippet."""

# Configure pythonpath and CWD
import os, sys
from pathlib import Path
from dotenv import find_dotenv

PROJ_ROOT = Path(find_dotenv("requirements.txt")).parent
sys.path.insert(0, str(PROJ_ROOT))
os.chdir(PROJ_ROOT)

import json

from utils.load_env import load_env

from vector_indexing.components.embedders.sagemaker import SageMakerEmbedder

load_env("config/.env.production")

DEFAULT_ENDPOINT = (
    "tei-pplx-embed-v1-4b-ml-g6e-xlarge-20260506233640"  # pragma: allowlist secret
)
DEFAULT_PROFILE = "vra-sandbox"

TEXT = "How can libraries improve discovery for digitized collections?"


embedder = SageMakerEmbedder(
    endpoint_name=DEFAULT_ENDPOINT,
    aws_profile=DEFAULT_PROFILE,
)

vector = embedder.embed_one(TEXT)

print(f"Endpoint: {DEFAULT_ENDPOINT}")
print(f"Profile: {DEFAULT_PROFILE}")
print(f"Vector length: {len(vector)}")
print("Vector preview (first 8 values):")
print(json.dumps(vector[:8], indent=2))
