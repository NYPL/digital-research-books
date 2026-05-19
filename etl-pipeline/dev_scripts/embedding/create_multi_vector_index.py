import os
import sys
from pathlib import Path
from dotenv import find_dotenv

PROJ_ROOT = Path(find_dotenv("requirements.txt")).parent
sys.path.insert(0, str(PROJ_ROOT))
os.chdir(PROJ_ROOT)

from utils.load_env import load_env

load_env("config/.env.production")
os.environ["STAGE"] = "development"
os.environ["LOG_LEVEL"] = "debug"

import turbopuffer
from vector_indexing.core.config import get_config


config = get_config()
client = turbopuffer.Turbopuffer(
    api_key=config.turbopuffer_api_key,
    region=config.turbopuffer_region,
)

# New vector columns to add alongside the existing "vector" field copied from vra-dev
NEW_VECTOR_FIELDS = {
    "harrier_oss_v1_vector": {"type": "[1024]f16", "ann": True},  # harrier-oss-v1-0.6b
    "qwen3_8b_vector": {"type": "[1024]f16", "ann": True},  # Qwen3-Embedding-8B
    "pplx_v1_4b_vector": {"type": "[1024]f16", "ann": True},  # pplx-embed-v1-4b
    "kalm_gemma3_12b_vector": {
        "type": "[1024]f16",
        "ann": True,
    },  # KaLM-Embedding-Gemma3-12B-2511
    "jina_v5_small_vector": {
        "type": "[1024]f16",
        "ann": True,
    },  # jina-embeddings-v5-text-small (0.6B)
}
# NOTE: Intentionally ignoring that TP docs say "A namespace can currently be
# created with up to 2 vector columns. The number of vector columns cannot be
# changed after namespace creation."
# https://turbopuffer.com/docs/write#vectors

MULTI_VECTOR_NAMESPACE = "vra-multi-vector"
SOURCE_NAMESPACE = "vra-dev"

# NOTE: in the multi-vector namespace, we will be charged for initial data copy,
# and we will be charged by storage for all filterable fields multiplied by the
# number of vector fields (notably not text field) and once for non-filterable fields


ns = client.namespace(MULTI_VECTOR_NAMESPACE)


# Step 1: copy existing data into the new namespace (destination must be empty;
# copy_from_namespace cannot include schema changes or documents)
print(ns.write(copy_from_namespace=SOURCE_NAMESPACE))


# Step 2: extend schema with new vector columns before writing new embeddings
print(ns.write(schema=NEW_VECTOR_FIELDS))


# TODO: patch-embed into multi-vector index

# NOTE: TurbopufferBackend.insert() currently writes to the "vector"
# column via chunk_to_tpuf_row(). Before running, update it (or add a
# vector_field_name param) so it writes to "harrier_oss_v1_vector" instead.
