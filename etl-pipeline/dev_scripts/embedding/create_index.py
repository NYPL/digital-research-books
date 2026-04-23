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
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from model.postgres.grin_public_domain_10k import GrinPublicDomain10k
from vector_indexing.core.config import get_config
from vector_indexing.components.backends.turbopuffer import (
    TurbopufferBackend,
    load_default_schema,
)
from vector_indexing.components.embedders.sagemaker import SageMakerEmbedder
from vector_indexing.components.loaders.s3 import CachedS3BookLoader
from vector_indexing.scripts.run_pipeline import run_pipeline


INDEX_NAME = "vra_test_harrier_harrier_oss_v1_.6b"
HARRIER_OSS_V1_ENDPOINT = (
    "PLACEHOLDER"  # harrier-oss-v1-0.6b  (1024-dimensional output)
)
HARRIER_OSS_V1_DIMENSIONS = 1024


# List barcodes to embed (from grin_public_domain_10k table)
config = get_config()
engine = create_engine(config.pg_connection_url)
with Session(engine) as db_session:
    rows = db_session.execute(select(GrinPublicDomain10k.barcode)).scalars().all()
barcodes = list(rows)  # Q: don't call list() to to allow lazy loading....?
print(f"Fetched {len(barcodes)} barcodes from grin_public_domain_10k")

# Run the indexing pipeline with the SageMaker (harrier) embedder
schema = load_default_schema()
schema["vector"]["type"] = f"[{HARRIER_OSS_V1_DIMENSIONS}]f16"
batch_result = run_pipeline(
    barcodes,
    loader=CachedS3BookLoader(),
    embedder=SageMakerEmbedder(
        endpoint_name=HARRIER_OSS_V1_ENDPOINT,
    ),
    backend=TurbopufferBackend(index_name=INDEX_NAME, schema=schema),
)

print(f"\n{batch_result}")
