"""Unit tests for S3BookLoader.

Tests the loading flow: cache -> pages -> archive fallback.
Uses mocked S3 client and GPG to avoid network/decryption dependencies.
"""

import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

from vector_indexing.core.types import Book
from vector_indexing.components.loaders.s3 import (
    S3BookLoader,
    _extract_page_number,
)
from vector_indexing.components.loaders.base import BookNotFoundError


# --- Fixtures ---


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client with common setup."""
    client = Mock()
    # Default: empty paginator (no pages found)
    paginator = Mock()
    paginator.paginate.return_value = []
    client.get_paginator.return_value = paginator
    return client


@pytest.fixture
def mock_cache():
    """Create a mock BookCache."""
    cache = Mock()
    cache.get.return_value = None
    cache.exists.return_value = False
    return cache


@pytest.fixture
def loader(mock_s3_client):
    """S3BookLoader with mocked S3 client and GPG."""
    with patch("vector_indexing.components.loaders.s3.gnupg.GPG"):
        return S3BookLoader(
            bucket="test-bucket",
            prefix="grin",
            s3_client=mock_s3_client,
            grin_access_key="test-key",
        )


@pytest.fixture
def loader_with_cache(mock_s3_client, mock_cache):
    """S3BookLoader with mocked S3 and cache."""
    with patch("vector_indexing.components.loaders.s3.gnupg.GPG"):
        return S3BookLoader(
            bucket="test-bucket",
            prefix="grin",
            s3_client=mock_s3_client,
            cache=mock_cache,
            grin_access_key="test-key",
        )


def make_xml_with_pages(page_count: int) -> str:
    """Generate mock XML metadata with given page count."""
    pages = "\n".join(
        f'<gbs:ocr_page FILEID="HTML{i:08d}"/>' for i in range(1, page_count + 1)
    )
    return f"""<?xml version="1.0"?>
    <METS:mets xmlns:gbs="http://books.google.com/gbs" xmlns:METS="http://www.loc.gov/METS/">
        <gbs:ocr>
            {pages}
        </gbs:ocr>
    </METS:mets>
    """


# --- Utility function tests ---


class TestExtractPageNumber:
    """Tests for _extract_page_number helper."""

    def test_simple_number(self):
        assert _extract_page_number("grin/123/00000042.txt") == 42

    def test_with_underscore_suffix(self):
        # Pattern like 1_1.txt extracts first number
        assert _extract_page_number("path/1_1.txt") == 1
        assert _extract_page_number("path/42_3.txt") == 42

    def test_nested_path(self):
        assert _extract_page_number("deep/nested/path/00001.txt") == 1

    def test_invalid_no_number(self):
        with pytest.raises(ValueError, match="Could not extract page number"):
            _extract_page_number("path/nonumber.txt")

    def test_invalid_no_txt(self):
        with pytest.raises(ValueError, match="Could not extract page number"):
            _extract_page_number("path/notext.pdf")


# --- S3BookLoader tests ---


class TestBuildS3Prefix:
    """Tests for _build_s3_prefix."""

    def test_strips_trailing_slash(self, mock_s3_client):
        with (
            patch("vector_indexing.components.loaders.s3.gnupg.GPG"),
            patch("vector_indexing.components.loaders.s3.get_config") as mock_config,
        ):
            # Mock config to return prefix with trailing slash
            mock_config.return_value.s3_bucket = "test"
            mock_config.return_value.s3_prefix = "grin/"
            mock_config.return_value.grin_access_key = None
            loader = S3BookLoader(
                bucket="test", prefix="grin/", s3_client=mock_s3_client
            )
        assert loader._build_s3_prefix("123") == "grin/123/"

    def test_without_prefix(self, mock_s3_client):
        with (
            patch("vector_indexing.components.loaders.s3.gnupg.GPG"),
            patch("vector_indexing.components.loaders.s3.get_config") as mock_config,
        ):
            # Mock config to return empty prefix
            mock_config.return_value.s3_bucket = "test"
            mock_config.return_value.s3_prefix = ""
            mock_config.return_value.grin_access_key = None
            loader = S3BookLoader(bucket="test", prefix="", s3_client=mock_s3_client)
        assert loader._build_s3_prefix("123") == "123/"


class TestTryCache:
    """Tests for _try_cache method."""

    def test_no_cache_configured(self, loader):
        """Returns None when no cache is set."""
        assert loader._try_cache("123") is None

    def test_cache_miss(self, loader_with_cache, mock_cache):
        """Returns None on cache miss."""
        mock_cache.get.return_value = None
        assert loader_with_cache._try_cache("123") is None
        mock_cache.get.assert_called_once_with("123")

    def test_cache_hit(self, loader_with_cache, mock_cache):
        """Returns book on cache hit."""
        cached_book = Book(barcode="123", pages=["page1"])
        mock_cache.get.return_value = cached_book

        result = loader_with_cache._try_cache("123")

        assert result is cached_book


class TestGetExpectedPageCount:
    """Tests for _get_expected_page_count XML parsing."""

    def test_parses_xml_page_count(self, loader, mock_s3_client):
        """Extracts page count from gbs:ocr_page elements."""
        xml_content = make_xml_with_pages(42)
        mock_s3_client.get_object.return_value = {
            "Body": Mock(read=lambda: xml_content.encode("utf-8"))
        }

        count = loader._get_expected_page_count("123")

        assert count == 42
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="grin/123/NYPL_123.xml"
        )

    def test_returns_none_on_missing_xml(self, loader, mock_s3_client):
        """Returns None if XML file doesn't exist."""
        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey"}}, "GetObject"
        )

        count = loader._get_expected_page_count("123")

        assert count is None

    def test_returns_none_on_malformed_xml(self, loader, mock_s3_client):
        """Returns None if XML is malformed."""
        mock_s3_client.get_object.return_value = {
            "Body": Mock(read=lambda: b"<invalid>not valid xml")
        }

        count = loader._get_expected_page_count("123")

        assert count is None


class TestTryPages:
    """Tests for _try_pages method."""

    def test_no_pages_found(self, loader, mock_s3_client):
        """Returns None when no .txt files exist."""
        paginator = Mock()
        paginator.paginate.return_value = [{"Contents": []}]
        mock_s3_client.get_paginator.return_value = paginator

        result = loader._try_pages("123")

        assert result is None

    def test_page_count_mismatch_returns_none(self, loader, mock_s3_client):
        """Returns None when page count doesn't match XML."""
        # Setup: 3 txt files but XML says 10 pages
        paginator = Mock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "grin/123/00001.txt"},
                    {"Key": "grin/123/00002.txt"},
                    {"Key": "grin/123/00003.txt"},
                ]
            }
        ]
        mock_s3_client.get_paginator.return_value = paginator

        # XML says 10 pages
        xml_content = make_xml_with_pages(10)
        mock_s3_client.get_object.return_value = {
            "Body": Mock(read=lambda: xml_content.encode("utf-8"))
        }

        result = loader._try_pages("123")

        assert result is None

    def test_successful_page_download(self, loader, mock_s3_client):
        """Successfully downloads and assembles pages."""
        # Setup: 3 txt files, XML says 3 pages
        paginator = Mock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "grin/123/00001.txt"},
                    {"Key": "grin/123/00002.txt"},
                    {"Key": "grin/123/00003.txt"},
                ]
            }
        ]
        mock_s3_client.get_paginator.return_value = paginator

        # XML confirms 3 pages
        xml_content = make_xml_with_pages(3)

        # Mock get_object for both XML and page downloads
        def get_object_side_effect(Bucket, Key):
            if Key.endswith(".xml"):
                return {"Body": Mock(read=lambda: xml_content.encode("utf-8"))}
            else:
                page_num = _extract_page_number(Key)
                return {
                    "Body": Mock(read=lambda: f"Content of page {page_num}".encode())
                }

        mock_s3_client.get_object.side_effect = get_object_side_effect

        result = loader._try_pages("123")

        assert result is not None
        assert result.barcode == "123"
        assert len(result.pages) == 3
        assert result.pages[0] == "Content of page 1"

    def test_skips_validation_when_no_xml(self, loader, mock_s3_client):
        """Loads pages even if XML metadata is missing."""
        paginator = Mock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "grin/123/00001.txt"},
                    {"Key": "grin/123/00002.txt"},
                ]
            }
        ]
        mock_s3_client.get_paginator.return_value = paginator

        def get_object_side_effect(Bucket, Key):
            if Key.endswith(".xml"):
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            else:
                page_num = _extract_page_number(Key)
                return {"Body": Mock(read=lambda: f"Page {page_num}".encode())}

        mock_s3_client.get_object.side_effect = get_object_side_effect

        result = loader._try_pages("123")

        assert result is not None
        assert len(result.pages) == 2


class TestTryArchive:
    """Tests for _try_archive method."""

    def test_no_archive_found(self, loader, mock_s3_client):
        """Returns None when archive doesn't exist."""
        mock_s3_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadObject"
        )

        result = loader._try_archive("123")

        assert result is None

    def test_no_grin_key_returns_none(self, mock_s3_client):
        """Returns None when GRIN_ACCESS_KEY is not set."""
        with patch("vector_indexing.components.loaders.s3.gnupg.GPG"):
            loader = S3BookLoader(
                bucket="test-bucket",
                prefix="grin",
                s3_client=mock_s3_client,
                grin_access_key=None,  # No key
            )

        # Archive exists
        mock_s3_client.head_object.return_value = {}

        result = loader._try_archive("123")

        assert result is None


class TestLoad:
    """Tests for the main load() method and fallback chain."""

    def test_returns_cached_book(self, loader_with_cache, mock_cache):
        """Returns book from cache without hitting S3."""
        cached_book = Book(barcode="123", pages=["cached page"])
        mock_cache.get.return_value = cached_book

        result = loader_with_cache.load("123")

        assert result is cached_book
        # S3 should not be called
        loader_with_cache.s3.get_paginator.assert_not_called()

    def test_caches_book_after_pages_load(
        self, loader_with_cache, mock_s3_client, mock_cache
    ):
        """Caches book after successful load from pages."""
        mock_cache.get.return_value = None  # Cache miss

        # Setup successful page load
        paginator = Mock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "grin/123/00001.txt"}]}
        ]
        mock_s3_client.get_paginator.return_value = paginator

        def get_object_side_effect(Bucket, Key):
            if Key.endswith(".xml"):
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return {"Body": Mock(read=lambda: b"Page content")}

        mock_s3_client.get_object.side_effect = get_object_side_effect

        result = loader_with_cache.load("123")

        # Book should be cached
        mock_cache.put.assert_called_once()
        assert mock_cache.put.call_args[0][0] == "123"
        assert mock_cache.put.call_args[0][1].barcode == "123"

    def test_fallback_to_archive_when_pages_fail(self, loader, mock_s3_client):
        """Falls back to archive when page validation fails."""
        # Pages exist but count mismatch
        paginator = Mock()
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "grin/123/00001.txt"}]}  # 1 file
        ]
        mock_s3_client.get_paginator.return_value = paginator

        # XML says 10 pages (mismatch)
        xml_content = make_xml_with_pages(10)

        def get_object_side_effect(Bucket, Key):
            if Key.endswith(".xml"):
                return {"Body": Mock(read=lambda: xml_content.encode("utf-8"))}
            return {"Body": Mock(read=lambda: b"content")}

        mock_s3_client.get_object.side_effect = get_object_side_effect

        # Archive exists
        mock_s3_client.head_object.return_value = {}

        # Mock archive loading by patching _load_from_archive
        with patch.object(
            loader, "_load_from_archive", return_value=Book("123", ["archive page"])
        ) as mock_load_archive:
            result = loader.load("123")

        assert result.barcode == "123"
        assert result.pages == ["archive page"]
        mock_load_archive.assert_called_once()

    def test_raises_not_found_when_all_sources_fail(self, loader, mock_s3_client):
        """Raises BookNotFoundError when cache, pages, and archive all fail."""
        # No pages
        paginator = Mock()
        paginator.paginate.return_value = [{"Contents": []}]
        mock_s3_client.get_paginator.return_value = paginator

        # No archive
        mock_s3_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadObject"
        )

        with pytest.raises(BookNotFoundError) as exc_info:
            loader.load("123")

        assert exc_info.value.barcode == "123"
        assert "test-bucket" in str(exc_info.value)


class TestExists:
    """Tests for exists() method."""

    def test_returns_true_if_cached(self, loader_with_cache, mock_cache):
        """Returns True if book is in cache."""
        mock_cache.exists.return_value = True

        assert loader_with_cache.exists("123") is True
        # Should not hit S3
        loader_with_cache.s3.list_objects_v2.assert_not_called()

    def test_returns_true_if_in_s3(self, loader, mock_s3_client):
        """Returns True if objects exist in S3."""
        mock_s3_client.list_objects_v2.return_value = {"KeyCount": 1}

        assert loader.exists("123") is True

    def test_returns_false_if_not_in_s3(self, loader, mock_s3_client):
        """Returns False if no objects in S3."""
        mock_s3_client.list_objects_v2.return_value = {"KeyCount": 0}

        assert loader.exists("123") is False

    def test_returns_false_on_s3_error(self, loader, mock_s3_client):
        """Returns False on S3 errors."""
        mock_s3_client.list_objects_v2.side_effect = Exception("S3 error")

        assert loader.exists("123") is False
