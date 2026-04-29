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

from datetime import datetime, timezone

import turbopuffer
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from model.postgres.grin_public_domain_10k import GrinPublicDomain10k
from vector_indexing.core.config import PROJECT_ROOT, get_config, get_index_config
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


def list_10k_barcodes(start_from: str | None = None):
    """Return all barcodes from grin_public_domain_10k, sorted ascending.

    If start_from is provided, only barcodes >= start_from are returned.
    """
    config = get_config()
    engine = create_engine(config.pg_connection_url)
    with Session(engine) as db_session:
        query = select(GrinPublicDomain10k.barcode).order_by(
            GrinPublicDomain10k.barcode
        )
        if start_from is not None:
            # TODO: check that start_from is in barcode list
            query = query.where(GrinPublicDomain10k.barcode >= start_from)
        rows = db_session.execute(query).scalars().all()
    barcodes = list(rows)  # Q: don't call list() to to allow lazy loading....?
    print(f"Fetched {len(barcodes)} barcodes from grin_public_domain_10k")
    return barcodes


def rerun_indexing(results_dir: Path) -> Iterator[str]:
    """Generate failed barcodes from saved batch results in an indexing
    run results directory
    """
    # TODO: add length (optional) for progress
    from vector_indexing.pipeline.orchestrator import BatchResult

    for path in sorted(results_dir.glob("batch_result_*.json")):
        batch_result = BatchResult.load(path)
        yield from (r.barcode for r in batch_result.results if not r.success)


INDEXING_RESULTS_DIR = Path(__file__, "..", "indexing_results").resolve()


# ============================================================================
# Index Config
# ============================================================================
# INDEX_NAME = "vra_test-sketches_of_the_north_river-harrier_oss_v1_.6b"
INDEX_NAME = "vra_test-10k-harrier_oss_v1_.6b"
# INDEX_NAME = "vra_test-sketches_of_the_north-qwen3_embedding_8b" # pragma: allowlist secret


# ============================================================================
# Books config
# ============================================================================
barcodes = list_10k_barcodes(start_from="33433066574009")
# barcodes = list_10k_barcodes()
# barcodes = ["33433062509165"] # sketches_of_the_north_river
# barcodes = rerun_indexing(INDEXING_RESULTS_DIR / "20260427T015457Z")


# TODO: batched run function
# TODO: per step and per book timings (wait for orchestration?)
# TODO: run progress percent
# TODO: option abort run on first non-100% batch and save last successful barcode to re-start with start_from=


index_config = get_index_config(INDEX_NAME)

# Define directories
run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
out_dir = INDEXING_RESULTS_DIR / run_timestamp

# for i, batch in enumerate(batched(barcodes, 100)):
for i, batch in enumerate(batched(barcodes, 100)):
    print(f"\nBatch {i + 1}: barcodes {batch[0]!r} .. {batch[-1]!r}")
    batch_result = run_pipeline(
        batch,
        # loader=LocalBookLoader(
        #     data_dir=PROJ_ROOT / "../.." / "vra_experiments/data/experiment_books"
        # ),
        embedder=index_config["embedder"],
        backend=index_config["backend"],
    )
    # TODO: add index config + book config  metadata save
    saved_path = batch_result.save(out_dir)
    # print(f"Saved: {saved_path}")
