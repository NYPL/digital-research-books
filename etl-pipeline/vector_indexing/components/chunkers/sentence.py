"""Sentence-based text chunker using LlamaIndex SentenceSplitter."""

from itertools import accumulate
from typing import Iterator, Optional

from llama_index.core.node_parser import SentenceSplitter

from vector_indexing.core.types import Book, BookMetadata, ChunkDocument
from vector_indexing.core.config import get_config, GlobalConfig
from vector_indexing.components.chunkers.base import TextChunker


class SentenceSplitterChunker(TextChunker):
    """Chunker using LlamaIndex's SentenceSplitter.
    Splits text at sentence boundaries while respecting token limits.
    Preserves paragraph structure where possible.
    Takes in:
        chunk_size: Target chunk size in tokens (default from config).
        chunk_overlap: Overlap between chunks in tokens (default from config).
        config: Optional GlobalConfig override.

    Example:
        chunker = SentenceSplitterChunker()
        for chunk in chunker.chunk(book):
            print(chunk.doc_id, chunk.start_page, chunk.end_page)
    """

    def __init__(
        self,
        chunk_size: Optional[int] = None,
        chunk_overlap: Optional[int] = None,
        config: Optional[GlobalConfig] = None,
    ):
        cfg = config or get_config()
        self._chunk_size = chunk_size or cfg.chunk_size
        self._chunk_overlap = chunk_overlap or cfg.chunk_overlap
        self._splitter = SentenceSplitter(
            chunk_size=self._chunk_size,
            chunk_overlap=self._chunk_overlap,
        )

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @property
    def chunk_overlap(self) -> int:
        return self._chunk_overlap

    def chunk(self, book: Book) -> Iterator[ChunkDocument]:
        """Split book into sentence-boundary chunks. Takes in a Book
        with populated pages and metadata. Yields chunk documents. Will raise
        a ValueError if book.metadata is None.
        """
        if book.metadata is None:
            raise ValueError(
                f"Book {book.barcode} has no metadata. "
                "Run metadata enrichment before chunking."
            )

        metadata = book.metadata

        # Join pages with newlines to preserve page structure.
        # NOTE: This creates a full copy of the book text in memory.
        # We should optimize this for very large books with a sliding
        # window approach that joins N pages at a time with overlap.
        pages_with_newlines = [page + "\n" for page in book.pages]
        book_text = "".join(pages_with_newlines)

        # Calculate cumulative end index of each page
        # (accounts for the newline added after each page)
        page_end_indices = list(accumulate(len(p) for p in pages_with_newlines))

        # Split into chunks
        chunks = self._splitter.split_text(book_text)

        # Track position through the text
        chunk_end_char = 0

        for chunk_index, chunk_text in enumerate(chunks):
            chunk_start_char = chunk_end_char
            chunk_end_char = chunk_start_char + len(chunk_text)

            # Map character positions to page numbers (1-indexed)
            start_page = self._char_to_page(chunk_start_char, page_end_indices)
            end_page = self._char_to_page(chunk_end_char, page_end_indices)

            yield ChunkDocument.create(
                barcode=book.barcode,
                book_id=book.book_id,
                chunk_index=chunk_index,
                text=chunk_text,
                start_page=start_page,
                end_page=end_page,
                book_metadata=metadata,
            )

    @staticmethod
    def _char_to_page(char_index: int, page_end_indices: list[int]) -> int:
        """Convert character index to 1-indexed page number.

        Args:
            char_index: Character position in the joined text.
            page_end_indices: Cumulative end index of each page.

        Returns:
            1-indexed page number containing the character.
        """
        # Count how many page boundaries we've passed
        # Page 1 is from 0 to page_end_indices[0]
        # Page 2 is from page_end_indices[0] to page_end_indices[1], etc.
        page_num = sum(char_index >= end_idx for end_idx in page_end_indices) + 1
        return page_num
