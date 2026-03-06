#!/usr/bin/env python
"""Patch page numbers for existing chunks in Turbopuffer.

This script re-runs the chunking logic on books and patches the start_page/end_page
fields in Turbopuffer without re-embedding or replacing entire documents.

Usage:
    # Patch specific barcodes:
    python -m vector_indexing.scripts.patch_page_numbers --barcodes 33433001234567 33433009876543

    # Patch barcodes from a file:
    python -m vector_indexing.scripts.patch_page_numbers --file barcodes.txt

    # Dry run:
    python -m vector_indexing.scripts.patch_page_numbers --barcodes 33433001234567 --dry-run
"""

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

# Add project root to path if running directly
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from vector_indexing.core.config import get_config
from vector_indexing.core.types import BookMetadata
from vector_indexing.components.chunkers import SentenceSplitterChunker
from vector_indexing.components.loaders import S3BookLoader
from vector_indexing.components.backends.turbopuffer import (
    TurbopufferBackend,
    TurbopufferPatchBuffer,
)


@dataclass
class BookPatchResult:
    """Result of patching a single book."""

    barcode: str
    success: bool
    chunks_patched: int
    chunks_skipped: int
    error: str | None = None

    def __repr__(self) -> str:
        status = "y" if self.success else "n"
        return f"BookPatchResult({status} {self.barcode}, patched={self.chunks_patched}, skipped={self.chunks_skipped})"


@dataclass
class BatchPatchResult:
    """Result of patching a batch of books."""

    results: list[BookPatchResult]
    total_patched: int = 0
    total_skipped: int = 0

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.success)

    def __repr__(self) -> str:
        return f"BatchPatchResult({self.succeeded}/{self.total} books, patched={self.total_patched}, skipped={self.total_skipped})"


def count_existing_chunks(backend: TurbopufferBackend, barcode: str) -> int:
    """Count how many chunks exist in Turbopuffer for a barcode."""
    count = 0
    # Use Glob filter on id to match "{barcode}_*"
    filters = ["id", "Glob", f"{barcode}_*"]
    for _ in backend.scan(filters=filters):
        count += 1
    return count


def patch_book_pages(
    barcode: str,
    loader: S3BookLoader,
    chunker: SentenceSplitterChunker,
    buffer: TurbopufferPatchBuffer,
    backend: TurbopufferBackend,
    dry_run: bool = False,
) -> BookPatchResult:
    """Load a book, re-chunk it, and patch page numbers.

    Args:
        barcode: Book barcode to process.
        loader: Book loader (S3 or local).
        chunker: Text chunker with updated page logic.
        buffer: Patch buffer for batching.
        backend: Turbopuffer backend for verification.
        dry_run: If True, don't actually patch.

    Returns:
        BookPatchResult with success/failure details.
    """
    try:
        # Load book
        book = loader.load(barcode)

        # We need metadata for chunking but we're only patching page numbers,
        # so use a minimal placeholder
        book.metadata = BookMetadata(
            edition_id=0,
            title="",
            author=[],
            subject=[],
            publication_date="",
            language=[],
        )

        # Collect all chunks first (needed for verification)
        chunks = list(chunker.chunk(book))

        # Verify chunk count matches before patching
        existing_count = count_existing_chunks(backend, barcode)
        if existing_count != len(chunks):
            return BookPatchResult(
                barcode=barcode,
                success=False,
                chunks_patched=0,
                chunks_skipped=0,
                error=f"Chunk count mismatch: {existing_count} in DB vs {len(chunks)} generated",
            )

        # Re-chunk to get new page numbers
        chunks_patched = 0
        chunks_skipped = 0

        for chunk in chunks:
            patch = {
                "id": chunk.doc_id,
                "start_page": chunk.start_page,
                "end_page": chunk.end_page,
            }

            if dry_run:
                print(
                    f"    Would patch {chunk.doc_id}: start_page={chunk.start_page}, end_page={chunk.end_page}"
                )
                chunks_patched += 1
            else:
                result = buffer.add(patch)
                if result:
                    # Buffer flushed - accumulate counts
                    chunks_patched += result.patched
                    chunks_skipped += result.skipped

        return BookPatchResult(
            barcode=barcode,
            success=True,
            chunks_patched=chunks_patched,
            chunks_skipped=chunks_skipped,
        )

    except Exception as e:
        return BookPatchResult(
            barcode=barcode,
            success=False,
            chunks_patched=0,
            chunks_skipped=0,
            error=str(e),
        )


def load_checkpoint(checkpoint_path: Path | None) -> set[str]:
    """Load already-patched barcodes from checkpoint file."""
    if checkpoint_path is None or not checkpoint_path.exists():
        return set()

    completed = set()
    for line in checkpoint_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            completed.add(line)
    return completed


def save_to_checkpoint(checkpoint_file: TextIO | None, barcode: str) -> None:
    """Append a barcode to the checkpoint file."""
    if checkpoint_file is not None:
        checkpoint_file.write(f"{barcode}\n")
        checkpoint_file.flush()  # Ensure it's written immediately


def save_to_failures(failures_file: TextIO | None, barcode: str, error: str) -> None:
    """Append a failed barcode and error to the failures file."""
    if failures_file is not None:
        failures_file.write(f"{barcode}\t{error}\n")
        failures_file.flush()


def patch_books(
    barcodes: list[str],
    loader: S3BookLoader,
    chunker: SentenceSplitterChunker,
    backend: TurbopufferBackend,
    dry_run: bool = False,
    checkpoint_file: TextIO | None = None,
    failures_file: TextIO | None = None,
) -> BatchPatchResult:
    """Patch page numbers for a batch of books.

    Args:
        barcodes: List of barcodes to process.
        loader: Book loader.
        chunker: Text chunker.
        backend: Turbopuffer backend.
        dry_run: If True, don't actually patch.
        checkpoint_file: File to write successful barcodes to.
        failures_file: File to write failed barcodes to.

    Returns:
        BatchPatchResult with individual results.
    """
    results = []

    with TurbopufferPatchBuffer(backend) as buffer:
        for i, barcode in enumerate(barcodes, 1):
            print(f"[{i}/{len(barcodes)}] Processing {barcode}...")
            result = patch_book_pages(
                barcode,
                loader,
                chunker,
                buffer,
                backend=backend,
                dry_run=dry_run,
            )
            results.append(result)
            print(f"  {result}")
            if result.success:
                save_to_checkpoint(checkpoint_file, barcode)
            else:
                save_to_failures(
                    failures_file, barcode, result.error or "Unknown error"
                )

        # Final flush happens on context exit (no-op for dry_run)

    return BatchPatchResult(
        results=results,
        total_patched=buffer.total_patched,
        total_skipped=buffer.total_skipped,
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Patch page numbers for existing chunks in Turbopuffer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Input source
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--barcodes",
        "-b",
        nargs="+",
        help="List of barcodes to patch",
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
        help="Print what would be patched without actually patching",
    )

    # Config overrides
    parser.add_argument(
        "--namespace",
        default="vra-dev",
        help="Turbopuffer namespace name (default: vra-dev)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Override chunk size",
    )
    parser.add_argument(
        "--chunk-overlap",
        type=int,
        help="Override chunk overlap",
    )
    parser.add_argument(
        "--checkpoint",
        "-c",
        type=Path,
        help="Checkpoint file to track completed barcodes. Skips barcodes already in file, appends new ones.",
    )
    parser.add_argument(
        "--failures",
        type=Path,
        help="File to write failed barcodes to (format: barcode<tab>error).",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Load config
    config = get_config()

    # Apply config overrides
    if args.chunk_size or args.chunk_overlap:
        from dataclasses import replace

        overrides = {}
        if args.chunk_size:
            overrides["chunk_size"] = args.chunk_size
        if args.chunk_overlap:
            overrides["chunk_overlap"] = args.chunk_overlap
        config = replace(config, **overrides)

    namespace = args.namespace
    print(
        f"Config: namespace={namespace}, chunk_size={config.chunk_size}, chunk_overlap={config.chunk_overlap}"
    )

    # Create backend
    backend = TurbopufferBackend.from_config(namespace, config)
    print(f"Using Turbopuffer namespace: {namespace}")

    # Load barcodes
    if args.barcodes:
        barcodes = args.barcodes
    else:  # args.file
        barcodes = [
            line.strip()
            for line in args.file.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]

    # Load checkpoint and filter out already-completed barcodes
    completed_barcodes = load_checkpoint(args.checkpoint)
    if completed_barcodes:
        original_count = len(barcodes)
        barcodes = [b for b in barcodes if b not in completed_barcodes]
        print(
            f"Checkpoint: {len(completed_barcodes)} already completed, {original_count - len(barcodes)} skipped"
        )

    print(f"Processing {len(barcodes)} barcodes")

    if not barcodes:
        print("No barcodes to process")
        return

    # Create loader (uses cache if available)
    loader = S3BookLoader(config=config)
    print(f"Using S3BookLoader from s3://{config.s3_bucket}/{config.s3_prefix}")

    # Create chunker
    chunker = SentenceSplitterChunker(config=config)
    print(
        f"Using SentenceSplitterChunker chunk_size={chunker.chunk_size}, overlap={chunker.chunk_overlap}"
    )

    if args.dry_run:
        print("\nDRY RUN - showing what would be patched:\n")

    # Run patching with checkpoint file open for appending
    checkpoint_file = None
    failures_file = None
    try:
        if args.checkpoint and not args.dry_run:
            checkpoint_file = open(args.checkpoint, "a")
        if args.failures and not args.dry_run:
            failures_file = open(args.failures, "a")

        result = patch_books(
            barcodes=barcodes,
            loader=loader,
            chunker=chunker,
            backend=backend,
            dry_run=args.dry_run,
            checkpoint_file=checkpoint_file,
            failures_file=failures_file,
        )
    finally:
        if checkpoint_file:
            checkpoint_file.close()
        if failures_file:
            failures_file.close()

    print(f"\nDone: {result}")
    print(f"  Succeeded: {result.succeeded}/{result.total}")
    print(f"  Total patched: {result.total_patched}")
    print(f"  Total skipped: {result.total_skipped}")

    if result.failed > 0:
        print("\nFailed books:")
        for r in result.results:
            if not r.success:
                print(f"  {r.barcode}: {r.error}")


if __name__ == "__main__":
    main()
