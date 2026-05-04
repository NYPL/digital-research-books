"""S3 book loader with parallel download support.

Downloads books from S3 using parallel requests and integrates with
DiskBookCache for local caching.

Supports two S3 formats:
1. Unpacked pages: Individual .txt files (e.g., grin/{barcode}/1_1.txt)
2. Encrypted archives: .tar.gz.gpg files that need decryption (e.g., grin/{barcode}/{barcode}.tar.gz.gpg)
"""

from __future__ import annotations

import os
import re
import tarfile
import tempfile
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cached_property
from typing import Optional, TYPE_CHECKING

import boto3
import gnupg
from botocore.config import Config

from logger import create_log
from vector_indexing.core.types import Book
from vector_indexing.core.config import get_config, GlobalConfig
from vector_indexing.components.loaders.base import (
    BookLoader,
    BookCache,
    BookNotFoundError,
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

    Tries to load books in this order:
    1. Local cache (if configured. Validation check: at least one page file must be present)
    2. Unpacked .txt pages from S3 (Validation check: use only if page count matches XML metadata)
    3. Encrypted .tar.gz.gpg archive from S3 (decrypts and extracts)

    If cache is configured, each book loaded from S3 pages or S3 archive is
    overwritten in cache.

    Args:
        bucket: S3 bucket name. Defaults to config.s3_bucket.
        prefix: S3 key prefix for book data. Defaults to config.s3_prefix.
        cache: Optional BookCache for local caching.
        max_workers: Max parallel download threads. It is recommended that
            smax_pool_connections == max_workers.
        max_pool_connections: Max open connections in the boto3 connection pool.
            It is recommended that max_pool_connections == max_workers.
        s3_client: Optional boto3 S3 client.
        config: Optional GlobalConfig.
        grin_access_key: Key for decrypting GRIN archives.

    Examples:
        loader = S3BookLoader()
        book = loader.load("33433000127989")

        # With disk caching
        cache = DiskBookCache(Path("./cache/books"))
        loader = S3BookLoader(cache=cache)
        book = loader.load("33433000127989")
    """

    def __init__(
        self,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
        cache: Optional[BookCache] = None,
        max_workers: int = 30,
        max_pool_connections: int = 30,
        s3_client: Optional[S3Client] = None,
        config: Optional[GlobalConfig] = None,
        grin_access_key: Optional[str] = None,
    ):
        self._config = config or get_config()
        self._bucket = bucket or self._config.s3_bucket
        self._prefix = prefix or self._config.s3_prefix
        self._cache = cache
        self._max_workers = max_workers
        self._max_pool_connections = max_pool_connections
        self._s3_client = s3_client
        self._grin_access_key = grin_access_key or self._config.grin_access_key
        self._gpg = gnupg.GPG()

    @cached_property
    def s3(self) -> S3Client:
        """Lazily initialize S3 client."""
        if self._s3_client is not None:
            return self._s3_client
        config = Config(
            # TODO: maybe just set max_pool_connections directly from max_workers
            max_pool_connections=self._max_pool_connections,
            retries={"max_attempts": 3, "mode": "adaptive"},
        )
        return boto3.client("s3", config=config)

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
        """Find encrypted archive key for a barcode. Makes a network request to verify the file exists. Returns None if not found."""
        prefix = self._build_s3_prefix(barcode)
        archive_key = f"{prefix}{barcode}.tar.gz.gpg"

        try:
            self.s3.head_object(Bucket=self._bucket, Key=archive_key)
            return archive_key
        except Exception:
            return None

    def _get_expected_page_count(self, barcode: str) -> Optional[int]:
        """Get expected page count from XML metadata file.

        Looks for NYPL_{barcode}.xml and counts gbs:ocr_page elements.
        Returns None if XML not found or cannot be parsed.
        """
        prefix = self._build_s3_prefix(barcode)
        xml_key = f"{prefix}NYPL_{barcode}.xml"

        try:
            response = self.s3.get_object(Bucket=self._bucket, Key=xml_key)
            xml_content = response["Body"].read().decode("utf-8")

            # Count gbs:ocr_page elements
            root = ET.fromstring(xml_content)
            ns = {"gbs": "http://books.google.com/gbs"}
            ocr_pages = root.findall(".//gbs:ocr_page", ns)
            return len(ocr_pages)
        except Exception as e:
            logger.debug(f"Could not get expected page count for {barcode}: {e}")
            return None

    def _load_from_archive(self, barcode: str, archive_key: str) -> Optional[Book]:
        """Download, decrypt, and extract pages from an encrypted archive.

        Returns:
            Book if successful, None if archive cannot be loaded.
        """
        if not self._grin_access_key:
            logger.warning(
                f"Cannot decrypt archive for {barcode}: GRIN_ACCESS_KEY not set"
            )
            return None

        logger.info(f"Loading {barcode} from encrypted archive")

        with tempfile.TemporaryDirectory() as tmp_dir:
            encrypted_path = os.path.join(tmp_dir, f"{barcode}.tar.gz.gpg")
            decrypted_path = os.path.join(tmp_dir, f"{barcode}.tar.gz")

            # Download
            try:
                self.s3.download_file(
                    Bucket=self._bucket,
                    Key=archive_key,
                    Filename=encrypted_path,
                )
            except Exception as e:
                logger.error(f"Failed to download archive for {barcode}: {e}")
                return None

            # Decrypt
            try:
                with open(encrypted_path, "rb") as encrypted_file:
                    result = self._gpg.decrypt_file(
                        encrypted_file,
                        passphrase=self._grin_access_key,
                        output=decrypted_path,
                    )
                if not result.ok:
                    logger.error(
                        f"GPG decryption failed for {barcode}: {result.status}"
                    )
                    return None
            except Exception as e:
                logger.error(f"Failed to decrypt archive for {barcode}: {e}")
                return None

            # Extract
            try:
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
                                    pass  # Skip files without valid page numbers

                if not pages_dict:
                    logger.error(f"No .txt pages found in archive for {barcode}")
                    return None

                pages = [pages_dict[k] for k in sorted(pages_dict.keys())]
                logger.info(f"Extracted {len(pages)} pages from archive for {barcode}")
                return Book(barcode=barcode, pages=pages)

            except Exception as e:
                logger.error(f"Failed to extract archive for {barcode}: {e}")
                return None

    def _download_page(self, key: str) -> tuple[int, str]:
        """Download a single page from S3. Returns a tuple of (page_number, page_content)."""
        response = self.s3.get_object(Bucket=self._bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        page_num = _extract_page_number(key)
        return (page_num, content)

    def _load_from_pages(self, barcode: str, keys: list[str]) -> Optional[Book]:
        """Download individual page files from S3.

        Returns:
            Book if successful, None if validation fails or no pages found.
        """
        # Validate page count against XML metadata
        expected_count = self._get_expected_page_count(barcode)
        if expected_count is not None and len(keys) != expected_count:
            logger.warning(
                f"Page count mismatch for {barcode}: found {len(keys)} .txt files, "
                f"expected {expected_count} from XML metadata"
            )
            return None

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
            logger.warning(f"Failed to download some pages for {barcode}: {errors[:3]}")
            return None

        if not pages_dict:
            return None

        pages = [pages_dict[k] for k in sorted(pages_dict.keys())]
        logger.debug(f"Downloaded {len(pages)} pages for {barcode}")
        return Book(barcode=barcode, pages=pages)

    def _try_cache(self, barcode: str) -> Optional[Book]:
        """Try to load from cache."""
        # TODO: implement a cache validation step that checks that the local
        # cache pages match the S3 page count (similar to _try_cache())

        if self._cache is None:
            return None
        book = self._cache.get(barcode)
        if book:
            logger.debug(f"Cache hit for {barcode}")
        return book

    def _try_pages(self, barcode: str) -> Optional[Book]:
        """Try to load from unpacked .txt pages."""
        try:
            keys = self._list_page_keys(barcode)
        except Exception as e:
            logger.warning(f"Failed to list pages for {barcode}: {e}")
            return None

        if not keys:
            return None

        return self._load_from_pages(barcode, keys)

    def _try_archive(self, barcode: str) -> Optional[Book]:
        """Try to load from encrypted archive."""
        archive_key = self._find_archive_key(barcode)
        if not archive_key:
            return None
        return self._load_from_archive(barcode, archive_key)

    # TODO: look for speed ups in downloading books in parallel and maybe
    # multi-process decrypting
    def load(self, barcode: str) -> Book:
        """Load a book, trying sources in order: cache -> pages -> archive.

        Raises:
            BookNotFoundError: If book not found in any source.
        """
        # Try each source in order
        if book := self._try_cache(barcode):
            return book

        if book := self._try_pages(barcode):
            if self._cache:
                self._cache.put(barcode, book)
            return book

        if book := self._try_archive(barcode):
            if self._cache:
                self._cache.put(barcode, book)
            return book

        raise BookNotFoundError(
            barcode,
            f"Not found at s3://{self._bucket}/{self._build_s3_prefix(barcode)}",
        )

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
