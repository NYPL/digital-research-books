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
from dataclasses import replace
from pathlib import Path

# Add project root to path if running directly
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from vector_indexing import (
    get_config,
    SentenceSplitterChunker,
)
from vector_indexing.components.backends.turbopuffer import TurbopufferBackend
from vector_indexing.pipeline import Pipeline
from vector_indexing.components.loaders import S3BookLoader, LocalBookLoader
from vector_indexing.components.embedders import GoogleEmbedder
from vector_indexing.components.metadata import MetadataProvider

TURBOPUFFER_INDEX_NAME = "vra-dev"


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


def _default_on_progress(result):
    status = "y" if result.success else "n"
    print(
        f"  {status} {result.barcode}: {result.chunks_inserted} chunks"
        + (f" ({result.error})" if result.error else "")
    )


def run_pipeline(
    barcodes: list[str],
    *,
    config_overrides: dict | None = None,
    loader=None,
    chunker=None,
    embedder=None,
    metadata_provider=None,
    backend=None,
    on_progress=_default_on_progress,
):
    """Run Pipeline with defaults"""
    # Q: better to bring the print logging into the run_pipeline() scope rather
    # than main() so its available where ever run_pipeline is called?

    config = get_config()
    if config_overrides:
        config = replace(config, **config_overrides)
        # FUTURE: remove config_overrides and just pass non-default pipeline
        # step objects when non-default config is desired.

    if loader is None:
        loader = S3BookLoader(config=config)
    if chunker is None:
        chunker = SentenceSplitterChunker(config=config)
    if embedder is None:
        embedder = GoogleEmbedder()
    if metadata_provider is None:
        metadata_provider = MetadataProvider(config=config)
    if backend is None:
        backend = TurbopufferBackend.from_config(
            index_name=TURBOPUFFER_INDEX_NAME,
            config=config,
            # TODO: set default index name in env config files
        )

    pipeline = Pipeline(
        loader=loader,
        chunker=chunker,
        embedder=embedder,
        metadata_provider=metadata_provider,
        backend=backend,
    )

    result = pipeline.index_books(barcodes, on_progress=on_progress)

    print(f"\nDone: {result}")
    print(f"  Books Succeeded: {result.succeeded}/{result.total}")
    print(
        f"  Chunks Inserted: {result.total_chunks_inserted}/{result.total_chunks_created}"
    )

    return result


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the vector indexing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Input source
    input_group = parser.add_mutually_exclusive_group(required=True)
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

    return parser.parse_args()


def main():
    args = parse_args()

    # Load barcodes
    if args.barcodes:
        barcodes = args.barcodes
    else:
        barcodes = [
            line.strip()
            for line in args.file.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]

    print(f"Processing {len(barcodes)} barcodes")

    if args.dry_run:
        print("DRY RUN - would process these barcodes:")
        for barcode in barcodes:
            print(f"  {barcode}")
        return

    kwargs = {}

    # Only build and pass components that differ from run_pipeline()'s defaults
    if args.chunk_size:
        kwargs["chunker"] = SentenceSplitterChunker(chunk_size=args.chunk_size)
        print(f"Using SentenceSplitterChunker with chunk_size={args.chunk_size}")
    else:
        print("Using SentenceSplitterChunker (default)")

    if args.local:
        kwargs["loader"] = LocalBookLoader()
        print("Using LocalBookLoader (local)")
    else:
        print("Using S3BookLoader (default)")

    if args.mock_embedder:
        kwargs["embedder"] = MockEmbedder()
        print("Using MockEmbedder (random vectors)")
    else:
        print("Using GoogleEmbedder (default)")

    if args.index_name != TURBOPUFFER_INDEX_NAME:
        kwargs["backend"] = TurbopufferBackend(index_name=args.index_name)
        print(f"Using TurbopufferBackend for index {args.index_name}")
    else:
        print(f"Using TurbopufferBackend for index {TURBOPUFFER_INDEX_NAME} (default)")

    if args.mock_metadata:
        kwargs["metadata_provider"] = MockMetadataProvider()
        print("Using MockMetadataProvider")
    else:
        print("Using MetadataProvider (default)")

    print(f"\nIndexing {len(barcodes)} books...")

    result = run_pipeline(barcodes, **kwargs)

    if result.failed > 0:
        print("\nFailed books:")
        for r in result.results:
            if not r.success:
                print(f"  {r.barcode}: {r.error}")


if __name__ == "__main__":
    main()
