"""Abstract base classes for book loading.

BookLoader: Interface for loading books from any source (disk, S3, etc.)
BookCache: Interface for caching loaded books
"""

from abc import ABC, abstractmethod
from typing import Optional

from vector_indexing.core.types import Book


class BookLoader(ABC):
    """Abstract interface for loading books."""

    @abstractmethod
    def load(self, barcode: str) -> Book:
        """Load a book by barcode. Raises BookNotFoundError if book doesn't exist. BookLoadError if loading fails."""
        ...

    @abstractmethod
    def exists(self, barcode: str) -> bool:
        """Check if a book exists in this source. True if book exists and can be loaded."""
        ...


class BookCache(ABC):
    """Abstract interface for caching books. Used to avoid repeat downloads."""

    @abstractmethod
    def get(self, barcode: str) -> Optional[Book]:
        """Get a book from cache. None if not cached"""
        ...

    @abstractmethod
    def put(self, barcode: str, book: Book) -> None:
        """Store a book in cache."""
        ...

    @abstractmethod
    def exists(self, barcode: str) -> bool:
        """Check if a book is cached. True if cached, False otherwise."""
        ...

    def delete(self, barcode: str) -> bool:
        """Remove a book from cache. True if book was deleted, False if not found."""
        # noop implementation, subclasses override
        return False


class BookNotFoundError(Exception):
    """Raised when a book cannot be found."""

    def __init__(self, barcode: str, source: str = "unknown"):
        self.barcode = barcode
        self.source = source
        super().__init__(f"Book '{barcode}' not found in {source}")


class BookLoadError(Exception):
    """Raised when a book fails to load."""

    def __init__(self, barcode: str, reason: str):
        self.barcode = barcode
        self.reason = reason
        super().__init__(f"Failed to load book '{barcode}': {reason}")
