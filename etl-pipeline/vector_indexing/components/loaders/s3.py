"""S3 book loader with parallel download support.

Downloads books from S3 using parallel requests and integrates with
DiskBookCache for local caching.

Supports two S3 formats:
1. Unpacked pages: Individual .txt files (e.g., grin/{barcode}/1_1.txt)
2. Encrypted archives: .tar.gz.gpg files that need decryption (e.g., grin/{barcode}/{barcode}.tar.gz.gpg)
"""

from __future__ import annotations

import io
import os
import re
import tarfile
import tempfile
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cached_property
from typing import Optional, TYPE_CHECKING

import boto3

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

logger = logging.getLogger(__name__)

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
        grin_access_key: Optional[str] = None,
    ):
        self._config = config or get_config()
        self._bucket = bucket or self._config.s3_bucket
        self._prefix = prefix or self._config.s3_prefix
        self._cache = cache
        self._max_workers = max_workers
        self._s3_client = s3_client
        self._grin_access_key = grin_access_key or self._config.grin_access_key
        self._gpg = None  # Lazily initialized

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
        """List all page .txt keys for a barcode. Returns empty list if no pages found."""
        prefix = self._build_s3_prefix(barcode)
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".txt"):
                    keys.append(key)

        return keys

    def _find_archive_key(self, barcode: str) -> Optional[str]:
        """Find encrypted archive key for a barcode. Returns None if not found."""
        prefix = self._build_s3_prefix(barcode)
        archive_key = f"{prefix}{barcode}.tar.gz.gpg"

        try:
            self.s3.head_object(Bucket=self._bucket, Key=archive_key)
            return archive_key
        except Exception:
            return None

    def _get_gpg(self):
        """Lazily initialize GPG instance."""
        if self._gpg is None:
            try:
                import gnupg

                self._gpg = gnupg.GPG()
            except ImportError:
                raise BookLoadError(
                    "gnupg",
                    "gnupg package required for decrypting archives. Install with: pip install python-gnupg",
                )
        return self._gpg

    def _load_from_archive(self, barcode: str, archive_key: str) -> Book:
        """Download, decrypt, and extract pages from an encrypted archive.

        Args:
            barcode: Book barcode.
            archive_key: S3 key for the .tar.gz.gpg file.

        Returns:
            Book object with extracted pages.

        Raises:
            BookLoadError: If decryption or extraction fails.
        """
        if not self._grin_access_key:
            raise BookLoadError(
                barcode,
                "GRIN_ACCESS_KEY required to decrypt archives. Set the environment variable.",
            )

        logger.info(f"Loading {barcode} from encrypted archive: {archive_key}")

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Download encrypted archive
            encrypted_path = os.path.join(tmp_dir, f"{barcode}.tar.gz.gpg")
            decrypted_path = os.path.join(tmp_dir, f"{barcode}.tar.gz")

            try:
                logger.debug(f"Downloading archive for {barcode}")
                self.s3.download_file(
                    Bucket=self._bucket,
                    Key=archive_key,
                    Filename=encrypted_path,
                )
            except Exception as e:
                raise BookLoadError(barcode, f"Failed to download archive: {e}")

            # Decrypt
            try:
                logger.debug(f"Decrypting archive for {barcode}")
                gpg = self._get_gpg()
                with open(encrypted_path, "rb") as encrypted_file:
                    result = gpg.decrypt_file(
                        encrypted_file,
                        passphrase=self._grin_access_key,
                        output=decrypted_path,
                    )
                if not result.ok:
                    raise BookLoadError(
                        barcode, f"GPG decryption failed: {result.status}"
                    )
            except BookLoadError:
                raise
            except Exception as e:
                raise BookLoadError(barcode, f"Failed to decrypt archive: {e}")

            # Extract pages from tarball
            try:
                logger.debug(f"Extracting pages from archive for {barcode}")
                pages_dict: dict[int, str] = {}

                with tarfile.open(decrypted_path, mode="r|*") as tar:
                    for member in tar:
                        if member.isfile() and member.name.endswith(".txt"):
                            file_obj = tar.extractfile(member)
                            if file_obj:
                                content = file_obj.read().decode("utf-8")
                                try:
                                    page_num = _extract_page_number(member.name)
                                    pages_dict[page_num] = content
                                except ValueError:
                                    logger.warning(
                                        f"Could not extract page number from: {member.name}"
                                    )

                if not pages_dict:
                    raise BookLoadError(barcode, "No .txt pages found in archive")

                pages = [pages_dict[k] for k in sorted(pages_dict.keys())]
                logger.info(f"Extracted {len(pages)} pages from archive for {barcode}")

                return Book(barcode=barcode, pages=pages)

            except BookLoadError:
                raise
            except Exception as e:
                raise BookLoadError(barcode, f"Failed to extract archive: {e}")

    def _download_page(self, key: str) -> tuple[int, str]:
        """Download a single page from S3. Returns a tuple of (page_number, page_content)."""
        response = self.s3.get_object(Bucket=self._bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        page_num = _extract_page_number(key)
        return (page_num, content)

    def _load_from_pages(self, barcode: str, keys: list[str]) -> Book:
        """Load book from individual page files.

        Args:
            barcode: Book barcode.
            keys: List of S3 keys for .txt page files.

        Returns:
            Book object with downloaded pages.
        """
        pages_dict: dict[int, str] = {}
        errors = []

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

        if not pages_dict:
            raise BookLoadError(barcode, "No pages downloaded")

        pages = [pages_dict[k] for k in sorted(pages_dict.keys())]
        logger.debug(f"Downloaded {len(pages)} pages for barcode {barcode} from S3")

        return Book(barcode=barcode, pages=pages)

    def load(self, barcode: str) -> Book:
        """Load a book from S3, using cache if present.

        Supports two S3 formats:
        1. Unpacked pages: Individual .txt files (tries this first)
        2. Encrypted archives: .tar.gz.gpg files (falls back to this)

        Raises:
            BookNotFoundError: If book not found in S3 (neither pages nor archive).
            BookLoadError: If download/decryption/extraction fails.
        """
        # Check cache first
        if self._cache is not None:
            cached_book = self._cache.get(barcode)
            if cached_book is not None:
                logger.debug(f"Cache hit for barcode {barcode}")
                return cached_book

        # Try to find unpacked page files first
        try:
            keys = self._list_page_keys(barcode)
        except Exception as e:
            raise BookLoadError(barcode, f"Failed to list S3 pages: {e}")

        book = None

        if keys:
            # Found unpacked pages - download them
            book = self._load_from_pages(barcode, keys)
        else:
            # No pages found - check for encrypted archive
            archive_key = self._find_archive_key(barcode)
            if archive_key:
                book = self._load_from_archive(barcode, archive_key)
            else:
                prefix = self._build_s3_prefix(barcode)
                raise BookNotFoundError(
                    barcode,
                    f"No pages or archive found at s3://{self._bucket}/{prefix}",
                )

        # Store in cache
        if self._cache is not None and book is not None:
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
        grin_access_key: Optional[str] = None,
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
            grin_access_key=grin_access_key,
        )

    @property
    def disk_cache(self) -> DiskBookCache:
        return self._cache
