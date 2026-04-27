import os
import sys
from collections.abc import Iterator
from pathlib import Path
from dotenv import find_dotenv

PROJ_ROOT = Path(find_dotenv("requirements.txt")).parent
sys.path.insert(0, str(PROJ_ROOT))
os.chdir(PROJ_ROOT)

from utils.load_env import load_env

load_env("config/.env.production")
os.environ["STAGE"] = "development"
os.environ["LOG_LEVEL"] = "debug"

from datetime import datetime, timezone

import turbopuffer
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from model.postgres.grin_public_domain_10k import GrinPublicDomain10k
from vector_indexing.core.config import PROJECT_ROOT, get_config
from vector_indexing.components.backends.turbopuffer import (
    TurbopufferBackend,
    load_default_schema,
)
from vector_indexing.components.embedders.sagemaker import SageMakerEmbedder
from vector_indexing.components.loaders import CachedS3BookLoader, LocalBookLoader
from vector_indexing.scripts.run_pipeline import run_pipeline
from logger import configure_loggers
from utils.common import batched

configure_loggers(log_level="info", stage="development")


def list_10k_barcodes():
    config = get_config()
    engine = create_engine(config.pg_connection_url)
    with Session(engine) as db_session:
        rows = (
            db_session.execute(
                select(GrinPublicDomain10k.barcode).order_by(
                    GrinPublicDomain10k.barcode
                )
            )
            .scalars()
            .all()
        )
    barcodes = list(rows)  # Q: don't call list() to to allow lazy loading....?
    print(f"Fetched {len(barcodes)} barcodes from grin_public_domain_10k")
    return barcodes


def rerun_indexing(results_dir: Path) -> Iterator[str]:
    """Generate failed barcodes from saved batch results in an indexing
    run results directory
    """
    from vector_indexing.pipeline.orchestrator import BatchResult

    for path in sorted(results_dir.glob("batch_result_*.json")):
        batch_result = BatchResult.load(path)
        yield from (r.barcode for r in batch_result.results if not r.success)


INDEXING_RESULTS_DIR = Path(__file__, "..", "indexing_results").resolve()


# Index Config
# INDEX_NAME = "vra_test-sketches_of_the_north_river-harrier_oss_v1_.6b"
INDEX_NAME = "vra_test-10k-harrier_oss_v1_.6b"
HARRIER_OSS_V1_DIMENSIONS = 1024
# Embedder Config
HARRIER_OSS_V1_ENDPOINT = "hf-tei-harrier-oss-v1-0-6b-ml-g6-2xlarge-20260424-011130"  # pragma: allowlist secret
CONCURRENCY = 41
# Book set config
# barcodes = list_10k_barcodes()
# barcodes = ["33433062509165"]
barcodes = rerun_indexing(INDEXING_RESULTS_DIR / "20260427T015457Z")


# Run the indexing pipeline with the SageMaker (harrier) embedder
schema = load_default_schema()
schema["vector"]["type"] = f"[{HARRIER_OSS_V1_DIMENSIONS}]f16"

# Define directories
run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out_dir = INDEXING_RESULTS_DIR / run_timestamp

# for i, batch in enumerate(batched(barcodes, 100)):
for i, batch in enumerate(batched(barcodes, 100)):
    print(f"\nBatch {i + 1}: barcodes {batch[0]!r} .. {batch[-1]!r}")
    batch_result = run_pipeline(
        batch,
        loader=LocalBookLoader(
            data_dir=PROJ_ROOT / "../.." / "vra_experiments/data/experiment_books"
        ),
        embedder=SageMakerEmbedder(
            endpoint_name=HARRIER_OSS_V1_ENDPOINT,
            aws_profile="sandbox",
            concurrency=CONCURRENCY,
        ),
        backend=TurbopufferBackend(index_name=INDEX_NAME, schema=schema),
    )
    saved_path = batch_result.save(out_dir)
    # print(f"Saved: {saved_path}")
