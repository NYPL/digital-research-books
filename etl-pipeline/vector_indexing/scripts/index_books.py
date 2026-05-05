#!/usr/bin/env python
"""Run the vector indexing pipeline on a list of barcodes.

Indexing is run in batches. Results are saved in a Job>Batches hierarchy in --results-dir.
By default, each invocation creates a new job. Use --resume-latest to resume the most
recent matching job for a given --index-name + barcode input.
For resumed runs, later batches may rerun barcodes from previous batches if they failed.

Usage:
    # From barcodes:
    python -m vector_indexing.scripts.index_books --barcodes 33433001234567 33433009876543

    # From barcode file (one per line):
    python -m vector_indexing.scripts.index_books --file barcodes.txt

    # From grin_public_domain_10k table:
    python -m vector_indexing.scripts.index_books --10k --index-name vra_test-10k-harrier_oss_v1_.6b

    # Resume the latest matching job (same --index-name + barcode input):
    python -m vector_indexing.scripts.index_books --10k --index-name vra_test-10k-harrier_oss_v1_.6b --resume-latest

    # Dry run (no actual indexing):
    python -m vector_indexing.scripts.index_books --barcodes 33433001234567 --dry-run

    # Use local files:
    python -m vector_indexing.scripts.index_books --barcodes 33433001234567 --loader LocalBookLoader

    # Use mock embedder (for testing):
    python -m vector_indexing.scripts.index_books --barcodes 33433001234567 --mock-embedder
"""

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


# Add project root to path if running directly
if __name__ == "__main__":
    from dotenv import find_dotenv

    project_root = Path(
        find_dotenv("requirements.txt", raise_error_if_not_found=True)
    ).parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from vector_indexing import SentenceSplitterChunker
from vector_indexing.pipeline.orchestrator import BatchResult, Pipeline
from vector_indexing.components import loaders
from vector_indexing.core.config import (
    get_index_config,
    get_index_config_dict,
    load_from_module,
)
from model.postgres.grin_public_domain_10k import GrinPublicDomain10k
from utils.common import batched


TURBOPUFFER_INDEX_NAME = "vra-dev"
DEFAULT_RESULTS_DIR = Path(__file__).resolve().parent / "indexing_results"
JOB_METADATA_FILE = "job_metadata.json"
JOB_STATE_FILE = "job_state.json"


class MockEmbedder:
    """Mock embedder for testing - returns random vectors."""

    def __init__(self, dimensions: int | None = None):
        self.dimensions = dimensions or get_config().embedding_dimensions

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        import random

        return [[random.random() for _ in range(self.dimensions)] for _ in texts]


class MockMetadataProvider:
    """Mock metadata provider - returns empty metadata."""

    def get_metadata(self, barcodes: list[str]) -> dict:
        from vector_indexing.core.types import BookMetadata

        return {
            barcode: BookMetadata(
                edition_id=None,
                title=f"Test Book {barcode}",
                author=["Unknown"],
                subject=[],
                publication_date=None,
                language=["en"],
            )
            for barcode in barcodes
        }


#### Job Metadata Helpers ####
# TODO: might be cleaner as a job metadata class, takes `args` (index-name, \
# barcode input, resume-latests) and sets up dir and metadata


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_job_metadata(args: argparse.Namespace) -> dict[str, Any]:
    index_config_dict = get_index_config_dict(args.index_name)
    index_config = {k: v for k, v in index_config_dict.items() if k != "names"}
    if args.ten_k:
        barcode_input = {"type": "10k", "value": "grin_public_domain_10k"}
    elif args.barcodes:
        barcode_input = {"type": "barcodes", "value": sorted(args.barcodes)}
    else:
        barcode_input = {"type": "file", "value": str(args.file.resolve())}
    return {
        "index_config": index_config,
        "barcode_input": barcode_input,
    }


def compare_job_metadata(
    saved_metadata: dict[str, Any], current_metadata: dict[str, Any]
) -> tuple[bool, bool]:
    saved_index_json = json.dumps(
        saved_metadata.get("index_config", {}), sort_keys=True, default=str
    )
    current_index_json = json.dumps(
        current_metadata.get("index_config", {}), sort_keys=True, default=str
    )

    saved_input_json = json.dumps(
        saved_metadata.get("barcode_input", {}), sort_keys=True, default=str
    )
    current_input_json = json.dumps(
        current_metadata.get("barcode_input", {}), sort_keys=True, default=str
    )
    return (
        saved_index_json == current_index_json,
        saved_input_json == current_input_json,
    )


def write_job_metadata(job_dir: Path, metadata: dict[str, Any]) -> Path:
    path = job_dir / JOB_METADATA_FILE
    json_str = json.dumps(metadata, sort_keys=True, indent=2, default=str)
    path.write_text(json_str)
    return path


def read_job_metadata(job_dir: Path) -> dict[str, Any]:
    path = job_dir / JOB_METADATA_FILE
    return json.loads(path.read_text())


def read_job_state(job_dir: Path) -> dict[str, Any]:
    path = job_dir / JOB_STATE_FILE
    if not path.exists():
        return {"total_succeeded": 0, "total_elapsed_seconds": 0.0}
    return json.loads(path.read_text())


def write_job_state(
    job_dir: Path, total_succeeded: int, total_elapsed_seconds: float
) -> None:
    state = {
        "total_succeeded": total_succeeded,
        "total_elapsed_seconds": total_elapsed_seconds,
    }
    (job_dir / JOB_STATE_FILE).write_text(json.dumps(state, indent=2))


def create_job_dir(results_dir: Path, index_name: str) -> Path:
    timestamp = _iso_utc_now()
    job_dir = results_dir / f"{index_name}_job_{timestamp}"
    job_dir.mkdir(parents=True, exist_ok=False)
    return job_dir


# TODO: update to find latest job matching index-name + barcode config, directly
# reading job_metadata.json, rather than filtering on index_name only
def find_latest_job(results_dir: Path, index_name: str) -> Path | None:
    if not results_dir.exists():
        return None

    candidates: list[tuple[str, Path]] = []
    for path in results_dir.glob("*_job_*"):
        if not path.is_dir() or "_job_" not in path.name:
            continue
        idx, ts = path.name.rsplit("_job_", 1)
        if idx == index_name:
            candidates.append((ts, path))

    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


##############################


#### Barcode Discovery Helpers ####


def start_from_job(job_dir: Path) -> tuple[str | None, bool]:
    """Return first failed barcode in a indexing job folder, distinguishing
    between no barcodes attempted and all barcodes succeeded.

    Returns the first failed barcode in chronological batch order. If batches exist
    and all records succeeded, returns ``(None, True)``.
    """
    batch_dirs = sorted(p for p in job_dir.glob("batch_*") if p.is_dir())
    if not batch_dirs:
        return None, False

    found_any_batch_result = False
    for batch_dir in batch_dirs:
        for path in sorted(batch_dir.glob("batch_result_*.json")):
            found_any_batch_result = True
            batch_result = BatchResult.load(path)
            for result in batch_result.results:
                if not result.success:
                    return result.barcode, False

    if not found_any_batch_result:
        return None, False
    return None, True


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
            query = query.where(GrinPublicDomain10k.barcode >= start_from)
        rows = db_session.execute(query).scalars().all()
    barcodes = list(rows)
    print(f"Fetched {len(barcodes)} barcodes from grin_public_domain_10k")
    return barcodes


# TODO: query limit 1 per barcode and get the batch_size-th largest barcode to \
# accommodate that last successful batch given fail-fast indexing
def get_last_indexed_barcode(index_name: str) -> str | None:
    """Return the lexicographically largest barcode already indexed in the given
    turbopuffer namespace, matching the ascending sort order used in list_10k_barcodes.
    Returns None if the namespace is empty or has no indexed documents.
    """
    backend = TurbopufferBackend(index_name=index_name)
    results = backend.query(
        rank_by=("barcode", "desc"),
        top_k=1,
        include_attributes=["barcode"],
    )
    if not results:
        return None
    chunk, _ = results[0]
    return chunk.barcode


def rerun_indexing(results_dir: Path) -> Iterator[str]:
    """Generate failed barcodes from saved batch results in an indexing
    run results directory
    """
    # TODO: add length (optional) for progress
    from vector_indexing.pipeline.orchestrator import BatchResult

    for path in sorted(results_dir.glob("batch_result_*.json")):
        batch_result = BatchResult.load(path)
        yield from (r.barcode for r in batch_result.results if not r.success)


###################################


def _apply_start_from(barcodes: list[str], start_from: str | None) -> list[str]:
    if start_from is None:
        return barcodes
    try:
        start_idx = barcodes.index(start_from)
    except ValueError as exc:
        raise SystemExit(
            f"Cannot resume: failed barcode {start_from!r} is not present in the selected barcode input."
        ) from exc
    return barcodes[start_idx:]


def resolve_barcodes(args: argparse.Namespace, start_from: str | None) -> list[str]:
    if args.ten_k:
        return list_10k_barcodes(start_from=start_from)
    if args.barcodes:
        return _apply_start_from(sorted(args.barcodes), start_from)
    file_barcodes = sorted(load_barcodes_from_file(args.file))
    return _apply_start_from(file_barcodes, start_from)


def load_barcodes_from_file(file_path: Path) -> list[str]:
    return [
        line.strip()
        for line in file_path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]


# NOTE: purely for pretty/readable errors
def parse_loader_args(raw: str) -> dict:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON for --loader-args: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("--loader-args must deserialize to a JSON object")
    return parsed


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the vector indexing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Input source
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--10k",
        dest="ten_k",
        action="store_true",
        help="Index grin_public_domain_10k barcodes, resuming from most recent failed barcode",
    )
    input_group.add_argument(
        "--barcodes",
        "-b",
        nargs="+",
        help="List of barcodes to process",
    )
    input_group.add_argument(
        "--file",
        "-f",
        type=Path,
        help="File containing barcodes (one per line)",
    )

    # Mode options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without actually indexing",
    )
    parser.add_argument(
        "--loader",
        default="S3BookLoader",
        help="Loader class name from vector_indexing.components.loaders",
    )
    parser.add_argument(
        "--loader-args",
        default="{}",
        help="JSON object with constructor kwargs for the selected loader",
    )
    parser.add_argument(
        "--mock-embedder",
        action="store_true",
        help="Use mock embedder (random vectors) instead of real API",
    )
    parser.add_argument(
        "--mock-metadata",
        action="store_true",
        help="Use mock metadata instead of querying Postgres",
    )

    # Config overrides
    parser.add_argument(
        "--index-name",
        default=TURBOPUFFER_INDEX_NAME,  # ALT: import INDEX_NAME from api.assistant.agent
        type=str,
        help="Override IndexBackend index_name",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Override chunk size",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for pipeline runs. Use 0 for a single batch.",
    )
    # TODO: resolve relative paths to __file__
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_DIR,
        help="Base directory for indexing job artifacts",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore index-config mismatch with latest job and overwrite saved index_config. Does not override barcode_input mismatch.",
    )
    parser.add_argument(
        "--resume-latest",
        action="store_true",
        help="Resume the latest matching job for this --index-name and barcode input. If omitted, always start a new job.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Load env vars
    from utils.load_env import load_env

    load_env("config/.env.production")

    # configure project loggers
    from logger import configure_loggers

    configure_loggers(log_level="info", stage="development")

    results_dir = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    current_job_metadata = build_job_metadata(args)
    job_dir = None
    if args.resume_latest:
        job_dir = find_latest_job(results_dir, args.index_name)
    start_from = None

    if job_dir is None:
        # Start new job
        job_dir = create_job_dir(results_dir, args.index_name)
        write_job_metadata(job_dir, current_job_metadata)
        write_job_state(job_dir, total_succeeded=0, total_elapsed_seconds=0.0)
    else:
        # Resume job: find barcode to resume from + validate metadata continuity

        saved_job_metadata = read_job_metadata(job_dir)
        index_match, barcode_input_match = compare_job_metadata(
            saved_job_metadata, current_job_metadata
        )
        if not barcode_input_match:
            raise SystemExit(
                "Barcode input mismatch with latest job. Start a new job (different --results-dir or --index-name) for a different barcode source."
            )

        if not index_match:
            if not args.force:
                raise SystemExit(
                    "Index config mismatch with latest job. Use --force to overwrite saved index_config."
                )
            saved_job_metadata["index_config"] = current_job_metadata["index_config"]
            write_job_metadata(job_dir, saved_job_metadata)

        start_from, all_succeeded = start_from_job(job_dir)
        if all_succeeded:
            print("No failed barcodes found in latest job. All done already.")
            return

    job_state = read_job_state(job_dir)
    cumulative_succeeded = job_state["total_succeeded"]
    cumulative_elapsed = job_state["total_elapsed_seconds"]

    barcodes = resolve_barcodes(args, start_from)

    print(f"Processing {len(barcodes)} barcodes")
    print(f"Job directory: {job_dir}")

    if args.dry_run:
        print("DRY RUN - would process these barcodes:")
        for barcode in barcodes:
            print(f"  {barcode}")
        return

    index_config = get_index_config(args.index_name)

    # Build pipeline kwargs
    kwargs: dict = {}

    loader_args = parse_loader_args(args.loader_args)
    loader_cls = load_from_module(args.loader, loaders)
    kwargs["loader"] = loader_cls(**loader_args)
    print(f"Using {args.loader} with loader args: {loader_args}")

    # Only build and pass components that differ from Pipeline's defaults
    if args.chunk_size:
        kwargs["chunker"] = SentenceSplitterChunker(chunk_size=args.chunk_size)
        print(f"Using SentenceSplitterChunker with chunk_size={args.chunk_size}")
    else:
        print("Using SentenceSplitterChunker (default)")

    if args.mock_embedder:
        kwargs["embedder"] = MockEmbedder()
        print("Using MockEmbedder (random vectors)")
    else:
        kwargs["embedder"] = index_config["embedder"]
        print(f"Using embedder from index config for {args.index_name}")

    kwargs["backend"] = index_config["backend"]
    print(f"Using backend from index config for {args.index_name}")

    if args.mock_metadata:
        kwargs["metadata_provider"] = MockMetadataProvider()
        print("Using MockMetadataProvider")
    else:
        print("Using MetadataProvider (default)")

    print(f"\nIndexing {len(barcodes)} books...")

    batch_size = None if args.batch_size == 0 else args.batch_size
    if batch_size is not None and batch_size <= 0:
        raise ValueError("batch_size must be positive or None")

    # Create pipeline with optional component overrides
    pipeline = Pipeline(**kwargs)

    batch_iter = [barcodes] if batch_size is None else batched(barcodes, batch_size)
    total_batches = 1 if batch_size is None else math.ceil(len(barcodes) / batch_size)

    this_run_processed = 0
    this_run_succeeded = 0
    this_run_failed = 0

    # Run indexing (with batching and fail-fast logic)
    for batch_index, barcode_batch in enumerate(batch_iter, start=1):
        result = pipeline.index_books(barcode_batch)
        # TODO: per step and per book timings (wait for orchestration?)

        # Update run-level counters
        this_run_processed += result.total
        this_run_succeeded += result.succeeded
        this_run_failed += result.failed

        # Update and persist cumulative job state
        cumulative_succeeded += result.succeeded
        cumulative_elapsed += result.total_time or 0.0
        write_job_state(job_dir, cumulative_succeeded, cumulative_elapsed)

        # Save batch result
        batch_dir = job_dir / f"batch_{_iso_utc_now()}"
        batch_dir.mkdir(parents=True, exist_ok=False)
        saved_path = result.save(batch_dir)

        batch_pct = 100.0 * batch_index / total_batches
        print(
            f"[Batch {batch_index}/{total_batches} | {batch_pct:.0f}%] Saved batch result: {saved_path}"
        )

        if result.failed > 0:
            print("Fail-fast: encountered failed records, stopping.")
            for r in result.results:
                if not r.success:
                    print(f"  {r.barcode}: {r.error}")
            break

    prior_succeeded = cumulative_succeeded - this_run_succeeded
    print(f"\nDone:")
    print(f"  This run:  {this_run_succeeded}/{this_run_processed} books succeeded")
    if prior_succeeded > 0:
        print(
            f"  Job total: {cumulative_succeeded} succeeded ({prior_succeeded} from prior runs)"
        )

    if this_run_failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
