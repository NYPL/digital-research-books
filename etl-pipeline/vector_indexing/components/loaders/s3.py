"""S3 book loader with parallel download support.

Downloads books from S3 using parallel requests and integrates with
DiskBookCache for local caching.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cached_property
from typing import Optional, TYPE_CHECKING
import re

import boto3

from logger import create_log
from vector_indexing.core.types import Book
from vector_indexing.core.config import get_config, GlobalConfig
from vector_indexing.components.loaders.base import (
    BookLoader,
    BookCache,
    BookNotFoundError,
    BookLoadError,
)
from vector_indexing.components.loaders.local import DiskBookCache

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = create_log(__name__)

# Regex to extract page number from S3 key
# Matches patterns like: path/to/barcode/1_1.txt, path/00042.txt, etc.
PAGE_NUMBER_PATTERN = re.compile(r"(\d+)(?:_\d+)?\.txt$")


def _extract_page_number(key: str) -> int:
    """Extract page number from S3 object key. Raises ValueError if page number cannot be extracted."""
    match = PAGE_NUMBER_PATTERN.search(key)
    if not match:
        raise ValueError(f"Could not extract page number from key: {key}")
    return int(match.group(1))


class S3BookLoader(BookLoader):
    """Load books from S3 with parallel downloads.
    Fetches page files from S3 and assembles them into Book objects.
    Supports optional disk caching via DiskBookCache.
    Takes in:
        bucket: S3 bucket name. If not provided, uses config.s3_bucket.
        prefix: S3 key prefix for book data. If not provided, uses config.s3_prefix.
        cache: Optional BookCache for local caching.
        max_workers: Max parallel download threads. Default 10.
        s3_client: Optional boto3 S3 client. If not provided, creates one.
        config: Optional GlobalConfig.

    Examples:
        loader = S3BookLoader()
        book = loader.load("33433000127989")

        # With disk caching
        cache = DiskBookCache(Path("./cache/books"))
        loader = S3BookLoader(cache=cache)
        book = loader.load("33433000127989")  # Downloads from S3, caches locally
        book = loader.load("33433000127989")  # Loads from cache
    """

    def __init__(
        self,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        cache: Optional[BookCache] = None,
        max_workers: int = 10,
        s3_client: Optional[S3Client] = None,
        config: Optional[GlobalConfig] = None,
    ):
        self._config = config or get_config()
        self._bucket = bucket or self._config.s3_bucket
        self._prefix = prefix or self._config.s3_prefix
        self._cache = cache
        self._max_workers = max_workers
        self._s3_client = s3_client

    @cached_property
    def s3(self) -> S3Client:
        """Lazily initialize S3 client."""
        if self._s3_client is not None:
            return self._s3_client
        return boto3.client("s3")

    @property
    def bucket(self) -> str:
        """S3 bucket name."""
        return self._bucket

    @property
    def prefix(self) -> str:
        """S3 key prefix."""
        return self._prefix

    @property
    def cache(self) -> Optional[BookCache]:
        """Optional book cache."""
        return self._cache

    def _build_s3_prefix(self, barcode: str) -> str:
        """Build the S3 prefix for a barcode."""
        if self._prefix:
            return f"{self._prefix.rstrip('/')}/{barcode}/"
        return f"{barcode}/"

    def _list_page_keys(self, barcode: str) -> list[str]:
        """List all page keys for a barcode. Raises BookNotFoundError if no pages found."""
        prefix = self._build_s3_prefix(barcode)
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".txt"):
                    keys.append(key)

        if not keys:
            raise BookNotFoundError(barcode, f"s3://{self._bucket}/{prefix}")

        return keys

    def _download_page(self, key: str) -> tuple[int, str]:
        """Download a single page from S3. Returns a tuple of (page_number, page_content)."""
        response = self.s3.get_object(Bucket=self._bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        page_num = _extract_page_number(key)
        return (page_num, content)

    def load(self, barcode: str) -> Book:
        """Load a book from S3, using cache if present. Raises BookNotFoundError if book not found in S3.
        Raises BookLoadError if download fails.
        """
        # Check cache first
        if self._cache is not None:
            cached_book = self._cache.get(barcode)
            if cached_book is not None:
                logger.debug(f"Cache hit for barcode {barcode}")
                return cached_book

        # List pages in S3 (i.e. download task list)
        try:
            keys = self._list_page_keys(barcode)
        except BookNotFoundError:
            raise
        except Exception as e:
            raise BookLoadError(barcode, f"Failed to list S3 pages: {e}")

        # Download pages in parallel
        pages_dict: dict[int, str] = {}
        errors = []

        # using ThreadPoolExecutor for I/O bound tasks
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {executor.submit(self._download_page, key): key for key in keys}

            for future in as_completed(futures):
                key = futures[future]
                try:
                    page_num, content = future.result()
                    pages_dict[page_num] = content
                except Exception as e:
                    errors.append(f"{key}: {e}")

        if errors:
            raise BookLoadError(barcode, f"Failed to download pages: {errors[:5]}")

        pages = [pages_dict[k] for k in sorted(pages_dict.keys())]

        if not pages:
            raise BookLoadError(barcode, "No pages downloaded")

        logger.debug(f"Downloaded {len(pages)} pages for barcode {barcode} from S3")

        book = Book(barcode=barcode, pages=pages)

        # Store in cache
        if self._cache is not None:
            self._cache.put(barcode, book)
            logger.debug(f"Cached book {barcode}")

        return book

    def exists(self, barcode: str) -> bool:
        """Check if a book exists in S3. Also checks cache first if available. True if book exists in cache or S3.
        This will make a network call to S3 if not cached (via the list_objects_v2 API).
        """
        barcode = str(barcode)

        if self._cache is not None and self._cache.exists(barcode):
            return True

        prefix = self._build_s3_prefix(barcode)

        try:
            response = self.s3.list_objects_v2(
                Bucket=self._bucket,
                Prefix=prefix,
                MaxKeys=1,
            )
            return response.get("KeyCount", 0) > 0
        except Exception as e:
            logger.warning(f"Error checking S3 existence for {barcode}: {e}")
            return False


class CachedS3BookLoader(S3BookLoader):
    """S3 book loader with built-in disk caching. Convenience class that creates a DiskBookCache automatically."""

    def __init__(
        self,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        cache_dir: Optional[str] = None,
        max_workers: int = 10,
        s3_client: Optional[S3Client] = None,
        config: Optional[GlobalConfig] = None,
    ):
        config = config or get_config()

        from pathlib import Path

        cache_path = Path(cache_dir) if cache_dir else config.resolved_book_cache_dir
        disk_cache = DiskBookCache(cache_dir=cache_path, config=config)

        super().__init__(
            bucket=bucket,
            prefix=prefix,
            cache=disk_cache,
            max_workers=max_workers,
            s3_client=s3_client,
            config=config,
        )

    @property
    def disk_cache(self) -> DiskBookCache:
        return self._cache
