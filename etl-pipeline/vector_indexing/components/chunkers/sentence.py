"""Sentence-based text chunker using LlamaIndex SentenceSplitter."""

from itertools import accumulate
from typing import Iterator

from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import Document

from vector_indexing.components.chunkers.base import ChunkWithPages, TextChunker
from vector_indexing.core.types import Book, BookMetadata, ChunkDocument


def char_to_page(
    char_index: int, page_end_indices: list[int], exclusive: bool = False
) -> int:
    """Convert character index to 1-indexed page number.

    Args:
        char_index: Character position in the joined text.
        page_end_indices: Cumulative end index of each page.
        exclusive: If True, treats char_index as exclusive (one past the last char).
    """
    if exclusive:
        return sum(char_index > end for end in page_end_indices) + 1
    else:
        return sum(char_index >= end for end in page_end_indices) + 1


class SentenceSplitterChunker(TextChunker):
    """Chunker using LlamaIndex's SentenceSplitter.

    Splits text at sentence boundaries while respecting token limits.
    Preserves paragraph structure where possible.

    Args:
        chunk_size: Target chunk size in tokens (default: 512).
        chunk_overlap: Overlap between chunks in tokens (default: 50).

    Example:
        chunker = SentenceSplitterChunker()
        for chunk in chunker.chunk(book):
            print(chunk.doc_id, chunk.start_page, chunk.end_page)
    """

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
    ):
        self._chunk_size = chunk_size
        self._chunk_overlap = chunk_overlap
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
        """Split book into sentence-boundary chunks.

        Args:
            book: Book with populated pages and metadata.

        Yields:
            ChunkDocument for each chunk.

        Raises:
            ValueError: If book.metadata is None.
        """
        if book.metadata is None:
            raise ValueError(
                f"Book {book.barcode} has no metadata. "
                "Run metadata enrichment before chunking."
            )

        for chunk in self.iter_chunks(book.pages):
            yield ChunkDocument.create(
                barcode=book.barcode,
                book_id=book.book_id,
                chunk_index=chunk.index,
                text=chunk.text,
                start_page=chunk.start_page,
                end_page=chunk.end_page,
                book_metadata=book.metadata,
            )

    def iter_chunks(self, pages: list[str]) -> Iterator[ChunkWithPages]:
        """Split pages into chunks with page positions.

        Args:
            pages: List of page text strings.

        Yields:
            ChunkWithPages with index, text, start_page, end_page.
        """
        # Join pages with newlines (could be bad for memory, optimize if needed)
        pages_with_newlines = [page + "\n" for page in pages]
        full_text = "".join(pages_with_newlines)
        page_end_indices = list(accumulate(len(p) for p in pages_with_newlines))

        # Get nodes from LlamaIndex (includes char indices)
        doc = Document(text=full_text)
        nodes = self._splitter.get_nodes_from_documents([doc])

        for chunk_index, node in enumerate(nodes):
            yield ChunkWithPages(
                index=chunk_index,
                text=node.text,
                start_page=char_to_page(
                    node.start_char_idx, page_end_indices, exclusive=False
                ),
                end_page=char_to_page(
                    node.end_char_idx, page_end_indices, exclusive=True
                ),
            )
