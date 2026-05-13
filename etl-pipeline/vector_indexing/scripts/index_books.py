#!/usr/bin/env python
"""Run the vector indexing pipeline on a list of barcodes.

The pipeline results in the insertion of embedding + metadata ChunkDocuments
into the specified index. Each document insertion overwrites the inserted
document id in the index, other existing document ids are unaffected.

An error will be raised if --index-name not already configured in vector_indexing/core/config.py

Indexing is run in batches. Results are saved in a Job>Batches hierarchy in --results-dir.

By default, each invocation creates a new job. Use --resume-latest to resume the
most recent job for a given --index-name from the first never succeeded
barcode in the sort order. If the barcode input from the job being resumed does
not match the input specified on resumption, an error will occur.
For resumed runs, later batches may rerun barcodes from previous batches.

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

    # Use qa environment config:
    python -m vector_indexing.scripts.index_books --10k --index-name vra_test-10k-harrier_oss_v1_.6b --env qa

    # Override nested index config values at runtime:
    python -m vector_indexing.scripts.index_books --10k --index-name vra_test-10k-harrier_oss_v1_.6b --config-overrides '{"embedder.params.endpoint_name": "new-endpoint-name"}'
"""

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

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
    DELETE,
    get_config,
    get_index_config,
    get_index_config_dict,
    load_from_module,
)
from vector_indexing.utils.barcodes import list_10k_barcodes
from utils.common import batched


TURBOPUFFER_INDEX_NAME = "vra-dev"
DEFAULT_RESULTS_DIR = Path(__file__).resolve().parent / "indexing_results"
JOB_METADATA_FILE = "job_metadata.json"
JOB_STATE_FILE = "job_state.json"


class MockEmbedder:
    """Mock embedder for testing - returns random vectors."""

    def __init__(self, dimensions: int | None = None):
        self.dimensions = dimensions or get_config().embedding_dimensions

    def embed_document_batch(self, texts: list[str]) -> list[list[float]]:
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


def build_job_metadata(
    args: argparse.Namespace, config_overrides: dict[str, Any]
) -> dict[str, Any]:
    index_config_dict = get_index_config_dict(
        args.index_name, overrides=config_overrides
    )
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
        return {
            "n_barcodes": 0,
            "total_attempts": 0,
            "total_successes": 0,
            "total_elapsed_seconds": 0.0,
        }
    return json.loads(path.read_text())


# TODO: move n_barcodes to job_metadata as it is fixed
def write_job_state(
    job_dir: Path,
    n_barcodes: int,  # TODO: since n_barcodes is set on job init, move to job_metadata
    total_attempts: int,
    total_successes: int,
    total_elapsed_seconds: float,
) -> None:
    state = {
        "n_barcodes": n_barcodes,
        "total_attempts": total_attempts,
        "total_successes": total_successes,
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


def ever_succeeded(job_dir: Path) -> set[str]:
    """Return the set of barcodes that have ever succeeded in the given job directory."""
    succeeded: set[str] = set()
    for batch_dir in sorted(p for p in job_dir.glob("batch_*") if p.is_dir()):
        for path in sorted(batch_dir.glob("batch_result_*.json")):
            for result in BatchResult.load(path).results:
                if result.success:
                    succeeded.add(result.barcode)
    return succeeded


def filter_from_job(job_dir: Path, all_barcodes: list[str]) -> list[str]:
    """Return barcodes from all_barcodes that have never succeeded in the given
    job directory, preserving input order.
    """
    succeeded = ever_succeeded(job_dir)
    return [b for b in all_barcodes if b not in succeeded]


def resolve_barcodes(args: argparse.Namespace) -> list[str]:
    if args.ten_k:
        return list_10k_barcodes()
    if args.barcodes:
        return sorted(args.barcodes)
    return sorted(load_barcodes_from_file(args.file))


def load_barcodes_from_file(file_path: Path) -> list[str]:
    return [
        line.strip()
        for line in file_path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]


def parse_json_object_arg(raw: str, arg_name: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid JSON object. json.JSONDecodeError: {exc}."
        ) from exc
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("Must deserialize to a JSON object.")
    return parsed


def parse_config_overrides(raw: str) -> dict[str, Any]:
    """Parse --config-overrides JSON, mapping the string "DELETE" to the DELETE sentinel."""
    parsed = parse_json_object_arg(raw, "config-overrides")
    return {k: DELETE if v == "DELETE" else v for k, v in parsed.items()}


def parse_loader_args(raw: str) -> dict[str, Any]:
    return parse_json_object_arg(raw, "loader-args")


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
        "--force",
        action="store_true",
        help="Ignore index-config mismatch with latest job and overwrite saved index_config. Does not override barcode_input mismatch.",
    )
    parser.add_argument(
        "--resume-latest",
        action="store_true",
        help="Resume the latest matching job for this --index-name and barcode input. If omitted, always start a new job.",
    )
    parser.add_argument(
        "--env",
        default="production",
        help="Environment name used to load config/.env.<env> (default: production)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for pipeline runs. Use 0 for a single batch.",
    )
    # TODO: resolve relative paths to __file__
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_DIR,
        help="Base directory for indexing job artifacts",
    )

    # Index Config
    parser.add_argument(
        "--index-name",
        default=TURBOPUFFER_INDEX_NAME,  # ALT: import INDEX_NAME from api.assistant.agent
        type=str,
        help="Override IndexBackend index_name",
    )
    parser.add_argument(
        "--config-overrides",
        type=parse_config_overrides,
        default={},
        help=(
            "JSON object of dotted-path overrides applied to index config before pipeline "
            'construction, e.g. {"embedder.params.endpoint_name": "new-endpoint-name"}. '
            'Use the string "DELETE" as a value to remove that path from the config, '
            'e.g. {"embedder.params.task_type": "DELETE"}'
        ),
    )

    # Pipeline steps not covered by INDEX_CONFIG
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Override chunk size",
    )
    parser.add_argument(
        "--loader",
        default="S3BookLoader",
        help="Loader class name from vector_indexing.components.loaders",
    )
    parser.add_argument(
        "--loader-args",
        type=parse_loader_args,
        default={},
        help=(
            "JSON object with constructor kwargs for the selected loader "
            'e.g. \'{"data_dir": "/path/to/books"}\''
        ),
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

    return parser.parse_args()


def main():
    args = parse_args()

    # Load env vars
    from utils.load_env import load_env

    load_env(f"config/.env.{args.env}")

    # configure project loggers
    from logger import configure_loggers

    configure_loggers(log_level="info", stage="development")

    results_dir = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    current_job_metadata = build_job_metadata(args, args.config_overrides)
    job_dir = None
    if args.resume_latest:
        job_dir = find_latest_job(results_dir, args.index_name)

    all_barcodes = resolve_barcodes(args)

    if job_dir is None:
        # Start new job (including resuming job that does not exist)
        job_dir = create_job_dir(results_dir, args.index_name)
        write_job_metadata(job_dir, current_job_metadata)
        write_job_state(
            job_dir,
            n_barcodes=len(all_barcodes),
            total_attempts=0,
            total_successes=0,
            total_elapsed_seconds=0.0,
        )
        barcodes = all_barcodes
        if args.resume_latest:
            print("No job available to resume, starting new job...")
        else:
            print("Starting new job...")
    else:
        # Resume job: validate metadata continuity, then filter to never-succeeded

        saved_job_metadata = read_job_metadata(job_dir)
        index_config_match, barcode_input_match = compare_job_metadata(
            saved_job_metadata, current_job_metadata
        )
        if not barcode_input_match:
            # TODO: more error details including job_dir and the 2 barcode inputs
            raise SystemExit(
                "Barcode input mismatch with latest job. Remove --resume-latest or use a different --results-dir or --index-name"
            )

        if not index_config_match:
            if not args.force:
                raise SystemExit(
                    "Index config mismatch with latest job. Use --force to overwrite saved index_config."
                )
            saved_job_metadata["index_config"] = current_job_metadata["index_config"]
            write_job_metadata(job_dir, saved_job_metadata)

        barcodes = filter_from_job(job_dir, all_barcodes)
        # TODO: future: on error/complete save metadata to allow read of \
        # start_from from job metadata (without having to read all barcodes into\
        # memory first)
        if not barcodes:
            print("All barcodes already succeeded. All done.")
            return
        print(
            f"Resuming job from barcode '{barcodes[0]}': {len(barcodes)}/{len(all_barcodes)} remaining..."
        )

    job_state = read_job_state(job_dir)
    cumulative_attempts = job_state["total_attempts"]
    cumulative_successes = job_state["total_successes"]
    cumulative_elapsed = job_state["total_elapsed_seconds"]

    print(f"Processing {len(barcodes)} barcodes")
    print(f"Job directory: {job_dir}")

    if args.dry_run:
        print("DRY RUN - would process these barcodes:")
        for barcode in barcodes:
            print(f"  {barcode}")
        return

    index_config = get_index_config(args.index_name, overrides=args.config_overrides)

    # Build pipeline kwargs
    kwargs: dict = {}

    loader_args = args.loader_args
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

        # Save batch result
        # (batch results are the source of truth for resume; job state is for reporting)
        batch_dir = job_dir / f"batch_{_iso_utc_now()}"
        batch_dir.mkdir(parents=True, exist_ok=False)
        saved_path = result.save(batch_dir)

        # Update and persist cumulative job state
        # TODO: maybe these cumulative counts are pointless, remove
        cumulative_attempts += result.total
        cumulative_successes += result.succeeded
        cumulative_elapsed += result.total_time or 0.0
        write_job_state(
            job_dir,
            n_barcodes=len(all_barcodes),
            # TODO: save cumulatove_ not total_
            total_attempts=cumulative_attempts,
            total_successes=cumulative_successes,
            total_elapsed_seconds=cumulative_elapsed,
        )

        batch_pct = 100.0 * batch_index / total_batches
        print(
            f"[Batch {batch_index}/{total_batches} | {batch_pct:.0f}%] Saved batch result: {saved_path}"
        )

        if result.failed > 0:
            print("Fail-fast: encountered failed records, stopping.")
            break

    prior_successes = cumulative_successes - this_run_succeeded
    print("\nDone:")
    print(f"  This run:  {this_run_succeeded}/{this_run_processed} books succeeded")
    # TODO: print batch timing and per book timing
    if prior_successes > 0:
        print(
            f"  Job cumulative: {cumulative_successes} books succeed / {cumulative_attempts} books attempted"
        )
    # Final report of n completed
    n_ever_succeeded = len(ever_succeeded(job_dir))
    print(
        f"  Job total: {n_ever_succeeded}/{len(all_barcodes)} barcodes ever succeeded"
    )

    if this_run_failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
