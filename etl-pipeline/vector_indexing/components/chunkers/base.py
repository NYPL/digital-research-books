"""Abstract base class for text chunkers."""

from abc import ABC, abstractmethod
from typing import Iterator, NamedTuple

from vector_indexing.core.types import Book, ChunkDocument


class ChunkWithPages(NamedTuple):
    """A text chunk with its page span."""

    index: int
    text: str
    start_page: int
    end_page: int


class TextChunker(ABC):
    """Abstract base for text chunking strategies.
    Transforms a Book into an iterator of ChunkDocuments, preserving page boundary information.
    """

    @abstractmethod
    def chunk(self, book: Book) -> Iterator[ChunkDocument]:
        """Split a book into chunks with page tracking. Takes in a book object
        and yields ChunkDocument objects with start_page and end_page set.
        Should raise a value error if the book.metadata is None (i.e. not yet enriched).
        """
        ...
