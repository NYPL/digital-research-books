"""Book loaders for local filesystem and S3.

This module provides abstractions for loading books from various sources
with optional caching support.

Classes:
    BookLoader: Abstract base class for book loaders.
    BookCache: Abstract base class for book caches.
    LocalBookLoader: Load books from local filesystem.
    DiskBookCache: Cache books on local filesystem.
    S3BookLoader: Load books from S3 with parallel downloads.
    CachedS3BookLoader: S3 loader with automatic disk caching.

Exceptions:
    BookNotFoundError: Raised when a book cannot be found.
    BookLoadError: Raised when book loading fails.

Example:
    # Local loading
    from vector_indexing.components.loaders import LocalBookLoader

    # Relative paths are resolved against project root
    loader = LocalBookLoader(Path("./data/experiment_books"))
    book = loader.load("33433000127989")

    # S3 with caching
    from vector_indexing.components.loaders import CachedS3BookLoader

    # Relative cache_dir is resolved against project root
    loader = CachedS3BookLoader(cache_dir="./data/cache/books")
    book = loader.load("33433000127989")
"""

from vector_indexing.components.loaders.base import (
    BookLoader,
    BookCache,
    BookNotFoundError,
    BookLoadError,
)
from vector_indexing.components.loaders.local import (
    LocalBookLoader,
    DiskBookCache,
)
from vector_indexing.components.loaders.s3 import (
    S3BookLoader,
    CachedS3BookLoader,
)

__all__ = [
    # Base classes
    "BookLoader",
    "BookCache",
    # Exceptions
    "BookNotFoundError",
    "BookLoadError",
    # Local
    "LocalBookLoader",
    "DiskBookCache",
    # S3
    "S3BookLoader",
    "CachedS3BookLoader",
]
