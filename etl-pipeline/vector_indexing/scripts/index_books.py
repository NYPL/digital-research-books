#!/usr/bin/env python
"""Run the vector indexing pipeline on a list of barcodes.

Usage:
    # With env vars loaded (from project root):
    python -m vector_indexing.scripts.index_books --barcodes 33433001234567 33433009876543

    # Or with a file of barcodes (one per line):
    python -m vector_indexing.scripts.index_books --file barcodes.txt

    # Auto-mode: resume from latest failed barcode for index
    python -m vector_indexing.scripts.index_books --auto --index-name vra_test-10k-harrier_oss_v1_.6b

    # Dry run (no actual indexing):
    python -m vector_indexing.scripts.index_books --barcodes 33433001234567 --dry-run

    # Use local files:
    python -m vector_indexing.scripts.index_books --barcodes 33433001234567 --loader LocalBookLoader

    # Use mock embedder (for testing):
    python -m vector_indexing.scripts.index_books --barcodes 33433001234567 --mock-embedder
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session


# Add project root to path if running directly
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
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
JOB_METADATA_FILE = "index_config.json"


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


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def compare_index_configs(config1: dict, config2: dict) -> bool:
    def exclude_names(cfg):
        return {k: v for k, v in cfg.items() if k != "names"}

    c1_json = json.dumps(exclude_names(config1), sort_keys=True, default=str)
    c2_json = json.dumps(exclude_names(config2), sort_keys=True, default=str)

    return c1_json == c2_json


def write_job_index_config(job_dir: Path, config_dict: dict) -> Path:
    path = job_dir / JOB_METADATA_FILE
    json_str = json.dumps(config_dict, sort_keys=True, indent=2, default=str)
    path.write_text(json_str)
    return path


def read_job_index_config(job_dir: Path) -> dict:
    path = job_dir / JOB_METADATA_FILE
    return json.loads(path.read_text())


def create_job_dir(results_dir: Path, index_name: str) -> Path:
    timestamp = _iso_utc_now()
    job_dir = results_dir / f"{index_name}_job_{timestamp}"
    job_dir.mkdir(parents=True, exist_ok=False)
    return job_dir


# TODO: make more concise
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


def start_from_job(job_dir: Path) -> tuple[str | None, bool]:
    """Return restart state from saved batch results in a job directory.

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
        "--auto",
        action="store_true",
        help="Automatically index 10k barcodes, resuming from most recent failed barcode",
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
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=DEFAULT_RESULTS_DIR,
        help="Base directory for indexing job artifacts",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore error when auto-resuming job and current config does not match original config. Use current index config.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.auto and not args.index_name:
        raise SystemExit("--auto requires --index-name")

    results_dir = args.results_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    index_config_dict = get_index_config_dict(args.index_name)

    # Collect barcodes
    if args.auto:
        job_dir = find_latest_job(results_dir, args.index_name)
        if job_dir is None:
            job_dir = create_job_dir(results_dir, args.index_name)
            write_job_index_config(job_dir, index_config_dict)
            start_from = None
        else:
            saved_config = read_job_index_config(job_dir)
            if not compare_index_configs(saved_config, index_config_dict):
                if not args.force:
                    raise SystemExit(
                        "Index config mismatch with latest job. Use --force to overwrite saved config."
                    )
                write_job_index_config(job_dir, index_config_dict)

            start_from, all_succeeded = start_from_job(job_dir)
            if all_succeeded:
                print("No failed barcodes found in latest job. All done already.")
                return

        barcodes = list_10k_barcodes(start_from=start_from)
    elif args.barcodes:
        barcodes = args.barcodes
        job_dir = create_job_dir(results_dir, args.index_name)
        write_job_index_config(job_dir, index_config_dict)
    else:
        barcodes = load_barcodes_from_file(args.file)
        job_dir = create_job_dir(results_dir, args.index_name)
        write_job_index_config(job_dir, index_config_dict)

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
    total_results = BatchResult()

    # Run indexing with batching and fail-fast logic
    for barcode_batch in batch_iter:
        result = pipeline.index_books(barcode_batch)
        # TODO: maybe put the batching logic inside Pipeline for easier programmatic only access.

        total_results.results.extend(result.results)
        if total_results.total_time is None:
            total_results.total_time = 0.0
        if result.total_time is not None:
            total_results.total_time += result.total_time

        # Save batch
        batch_dir = job_dir / f"batch_{_iso_utc_now()}"
        batch_dir.mkdir(parents=True, exist_ok=False)
        saved_path = result.save(batch_dir)
        print(f"Saved batch result: {saved_path}")

        if result.total > 0:
            print(
                f"Batch complete: {result.results[0].barcode!r} .. {result.results[-1].barcode!r}"
            )
        else:
            print("Batch complete: empty batch")

        if result.failed > 0:
            print("Fail-fast: encountered failed records, stopping.")
            break

    print(f"\nDone: {total_results}")
    print(f"  Books Succeeded: {total_results.succeeded}/{total_results.total}")
    print(
        f"  Chunks Inserted: {total_results.total_chunks_inserted}/{total_results.total_chunks_created}"
    )

    if total_results.failed > 0:
        print("\nFailed books:")
        for r in total_results.results:
            if not r.success:
                print(f"  {r.barcode}: {r.error}")

        raise SystemExit(1)


if __name__ == "__main__":
    main()
