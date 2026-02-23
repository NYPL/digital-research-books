#!/usr/bin/env python
"""Run the vector indexing pipeline on a list of barcodes.

Usage:
    # With env vars loaded (from project root):
    python -m vector_indexing.scripts.run_pipeline --barcodes 33433001234567 33433009876543

    # Or with a file of barcodes (one per line):
    python -m vector_indexing.scripts.run_pipeline --file barcodes.txt

    # Dry run (no actual indexing):
    python -m vector_indexing.scripts.run_pipeline --barcodes 33433001234567 --dry-run

    # Use local files instead of S3:
    python -m vector_indexing.scripts.run_pipeline --barcodes 33433001234567 --local

    # Use mock embedder (for testing):
    python -m vector_indexing.scripts.run_pipeline --barcodes 33433001234567 --mock-embedder
"""

import argparse
import sys
from pathlib import Path

# Add project root to path if running directly
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

# Load environment variables from .env file
from utils.load_env import load_env

# Parse --environment early (before other imports that depend on env vars)
import os

_env = os.getenv("ENVIRONMENT", "local")
# Check argv for --environment flag
for i, arg in enumerate(sys.argv):
    if arg in ("--environment", "-e") and i + 1 < len(sys.argv):
        _env = sys.argv[i + 1]
        break

_env_file = Path(__file__).parent.parent.parent / f"config/.env.{_env}"
if _env_file.exists():
    load_env(_env_file)
else:
    print(f"Warning: env file not found: {_env_file}")

from vector_indexing import (
    get_config,
    Pipeline,
    ElasticsearchBackend,
    S3BookLoader,
    LocalBookLoader,
    SentenceSplitterChunker,
    QwenEmbedder,
)
from vector_indexing.components.metadata import MetadataProvider


class MockEmbedder:
    """Mock embedder for testing - returns random vectors."""

    def __init__(self, dimensions: int = 768):
        self.dimensions = dimensions

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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the vector indexing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Environment (parsed early, but declared here for --help)
    parser.add_argument(
        "--environment",
        "-e",
        default="local",
        help="Environment to load config from (loads config/.env.<environment>)",
    )

    # Mode options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without actually indexing",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Load books from local disk instead of S3",
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
        "--es-index",
        help="Override Elasticsearch index name",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Override chunk size",
    )

    return parser.parse_args()


def main(barcodes: list[str]):
    """Run the pipeline.

    Args:
        barcodes: List of barcodes to process (required).
    """
    # Parse command line args for config options
    args = parse_args()

    print(f"Processing {len(barcodes)} barcodes")

    # Load config from env
    config = get_config()

    # Apply overrides
    if args.es_index or args.chunk_size:
        from dataclasses import replace

        overrides = {}
        if args.es_index:
            overrides["es_index"] = args.es_index
        if args.chunk_size:
            overrides["chunk_size"] = args.chunk_size
        config = replace(config, **overrides)

    print(f"Config: es_index={config.es_index}, chunk_size={config.chunk_size}")

    if args.dry_run:
        print("DRY RUN - would process these barcodes:")
        for barcode in barcodes:
            print(f"  {barcode}")
        return

    # Build pipeline components
    if args.local:
        loader = LocalBookLoader(config=config)
        print(f"Using LocalBookLoader from {config.resolved_book_cache_dir}")
    else:
        loader = S3BookLoader(config=config)
        print(f"Using S3BookLoader from s3://{config.s3_bucket}/{config.s3_prefix}")

    chunker = SentenceSplitterChunker(config=config)

    if args.mock_embedder:
        embedder = MockEmbedder(config.embedding_dimensions)
        print("Using MockEmbedder (random vectors)")
    else:
        embedder = QwenEmbedder(config=config)
        print(f"Using QwenEmbedder model={config.qwen_model}")

    if args.mock_metadata:
        metadata_provider = MockMetadataProvider()
        print("Using MockMetadataProvider")
    else:
        metadata_provider = MetadataProvider(config=config)
        print(f"Using MetadataProvider at {config.pg_host}")

    backend = ElasticsearchBackend.from_config(config=config)
    print(f"Using ElasticsearchBackend at {config.es_url}")

    # Create pipeline
    pipeline = Pipeline(
        loader=loader,
        chunker=chunker,
        embedder=embedder,
        metadata_provider=metadata_provider,
        backend=backend,
    )

    # Run pipeline
    print(f"\nIndexing {len(barcodes)} books...")

    def on_progress(result):
        status = "✓" if result.success else "✗"
        # print(f"  {status} {result.barcode}: {result.chunks_inserted} chunks" +
        #      (f" ({result.error})" if result.error else ""))

    result = pipeline.index_books(barcodes, on_progress=on_progress)

    print(f"\nDone: {result}")
    print(f"  Succeeded: {result.succeeded}/{result.total}")
    print(f"  Chunks: {result.total_chunks_inserted}/{result.total_chunks_created}")

    """
    if result.failed > 0:
        print("\nFailed books:")
        for r in result.results:
            if not r.success:
                print(f"  {r.barcode}: {r.error}")
    """


if __name__ == "__main__":
    print(get_config())
    main(["33433000136972", "33433006239176"])
    # main(["33433071108306", "33433009163845"])
