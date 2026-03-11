#!/usr/bin/env python
"""Chunk a book from a folder of page files.

Usage:
    python scripts/chunk_book.py /path/to/book/

Each .txt file in the directory is treated as a page (sorted by filename).
"""

import sys
from pathlib import Path

# Add parent to path for imports when running as script
sys.path.insert(0, str(Path(__file__).parent.parent))

from vector_indexing.components.chunkers.sentence import SentenceSplitterChunker


def load_pages(directory: Path) -> list[str]:
    """Load .txt files as pages, sorted by filename."""
    files = sorted(directory.glob("*.txt"))
    if not files:
        raise ValueError(f"No .txt files found in {directory}")
    return [f.read_text() for f in files]


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python scripts/chunk_book.py <book_dir> [chunk_size] [chunk_overlap]"
        )
        print()
        print("  book_dir      Directory containing .txt page files")
        print("  chunk_size    Target chunk size in tokens (default: 512)")
        print("  chunk_overlap Overlap between chunks in tokens (default: 50)")
        sys.exit(1)

    book_dir = Path(sys.argv[1])
    chunk_size = int(sys.argv[2]) if len(sys.argv) > 2 else 512
    chunk_overlap = int(sys.argv[3]) if len(sys.argv) > 3 else 50

    if not book_dir.is_dir():
        print(f"Not a directory: {book_dir}")
        sys.exit(1)

    pages = load_pages(book_dir)
    print(f"Loaded {len(pages)} pages from {book_dir.name}")

    chunker = SentenceSplitterChunker(
        chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )
    print(f"Chunking with size={chunk_size}, overlap={chunk_overlap}")
    print("-" * 60)

    chunks = list(chunker.iter_chunks(pages))
    print(f"Generated {len(chunks)} chunks\n")

    for chunk in chunks:
        preview = chunk.text[:80].replace("\n", " ")
        print(
            f"Chunk {chunk.index}: pages {chunk.start_page}-{chunk.end_page} | {preview}..."
        )

    print("-" * 60)
    print(f"Total: {len(chunks)} chunks from {len(pages)} pages")


if __name__ == "__main__":
    main()
