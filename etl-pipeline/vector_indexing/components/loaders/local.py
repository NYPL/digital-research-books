"""Local filesystem book loader.

Loads books from a directory structure where each barcode is a subdirectory
containing page files (*.txt).
"""

from pathlib import Path
from typing import Optional

from logger import create_log
from vector_indexing.core.types import Book
from vector_indexing.core.config import get_config, GlobalConfig
from vector_indexing.components.loaders.base import (
    BookLoader,
    BookCache,
    BookNotFoundError,
    BookLoadError,
)

logger = create_log(__name__)


class LocalBookLoader(BookLoader):
    """Load books from local filesystem. Takes in a base data directory and an optional config.
    If config is not provided, uses get_config() to obtain global config.

    Expects directory structure:
        data_dir/
            {barcode}/
                page_001.txt
                page_002.txt
                ...

    Note: By default data_dir is config.resolved_book_cache_dir which is data/v2/books.
    """

    def __init__(
        self,
        data_dir: Optional[Path] = None,
        config: Optional[GlobalConfig] = None,
    ):
        self._config = config or get_config()
        self._data_dir = (
            Path(data_dir) if data_dir else self._config.resolved_book_cache_dir
        )

    @property
    def data_dir(self) -> Path:
        """Base directory for book data (contains barcode subdirectories)."""
        return self._data_dir

    def load(self, barcode: str) -> Book:
        """Load a book from local filesystem. BookNotFoundError if barcode directory doesn't exist.
        BookLoadError if directory exists but has no .txt files.
        """
        barcode = str(barcode)
        barcode_dir = self._data_dir / barcode

        if not barcode_dir.exists():
            raise BookNotFoundError(barcode, f"local:{self._data_dir}")

        if not barcode_dir.is_dir():
            raise BookLoadError(
                barcode, f"Path exists but is not a directory: {barcode_dir}"
            )

        # Sort to ensure correct page order
        txt_files = sorted(barcode_dir.glob("*.txt"))

        if not txt_files:
            raise BookLoadError(barcode, f"No .txt files found in {barcode_dir}")

        # Read all pages
        pages = []
        for file_path in txt_files:
            try:
                pages.append(file_path.read_text())
            except Exception as e:
                raise BookLoadError(barcode, f"Failed to read {file_path}: {e}")

        logger.debug(f"Loaded {len(pages)} pages for barcode {barcode}")

        return Book(barcode=barcode, pages=pages)

    def exists(self, barcode: str) -> bool:
        """Check if a book directory exists locally. True if barcode directory exists and contains .txt files."""
        barcode = str(barcode)
        barcode_dir = self._data_dir / barcode

        if not barcode_dir.is_dir():
            return False

        # check for at least one text file
        return any(barcode_dir.glob("*.txt"))

    def list_barcodes(self) -> list[str]:
        """List all available barcodes."""
        if not self._data_dir.exists():
            return []

        barcodes = []
        for path in self._data_dir.iterdir():
            if path.is_dir() and path.name.isdigit():
                # Verify it has .txt files
                if any(path.glob("*.txt")):
                    barcodes.append(path.name)

        return sorted(barcodes)


class DiskBookCache(BookCache):
    """Cache books on local filesystem. Takes in the cache directory and optional config.
    If config is not provided, uses get_config() to obtain global config.
    """

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        config: Optional[GlobalConfig] = None,
    ):
        self._config = config or get_config()
        self._cache_dir = (
            Path(cache_dir) if cache_dir else self._config.resolved_book_cache_dir
        )

    @property
    def cache_dir(self) -> Path:
        """Directory for cached books (contains barcode subdirectories)."""
        return self._cache_dir

    def get(self, barcode: str) -> Optional[Book]:
        """Get a book from disk cache. Book if cached, None otherwise."""
        # MAYBE: add some logic that checks the page count from S3 and makes
        # sure its the same in the local cache and invalidates the local cache if not

        barcode = str(barcode)
        barcode_dir = self._cache_dir / barcode

        if not barcode_dir.is_dir():
            return None

        txt_files = sorted(barcode_dir.glob("*.txt"))
        if not txt_files:
            return None

        try:
            pages = [f.read_text() for f in txt_files]
            return Book(barcode=barcode, pages=pages)
        except Exception as e:
            logger.warning(f"Failed to read cached book {barcode}: {e}")
            return None

    def put(self, barcode: str, book: Book) -> None:
        """Store a book in disk cache.
        Creates directory structure and writes pages as numbered .txt files.
        Existing files are overwritten.
        """
        barcode = str(barcode)
        barcode_dir = self._cache_dir / barcode

        barcode_dir.mkdir(parents=True, exist_ok=True)

        # write number with padded zeros
        for i, page in enumerate(book.pages):
            page_file = barcode_dir / f"page_{i:04d}.txt"
            page_file.write_text(page)

        logger.debug(f"Cached {len(book.pages)} pages for barcode {barcode}")

    def exists(self, barcode: str) -> bool:
        """Check if a book is in disk cache. True if cached, False otherwise.
        NOTE: is_dir() uses a stat() call which is very fast, the glob scans the actual directory contents so it's a bit slower
        the any() short-circuits on first match though. If this is too slow we can switch to an os.scandir() or just check that the directory
        exists.
        NOTE: I considered adding an in-memory cache here for the contents of the directory to avoid repeated filesystem calls, but that adds complexity
        around cache invalidation etc. and this is probably not a big bottleneck unless we see significant slowdowns with this approach.
        """
        barcode = str(barcode)
        barcode_dir = self._cache_dir / barcode

        if not barcode_dir.is_dir():
            return False

        return any(barcode_dir.glob("*.txt"))

    def delete(self, barcode: str) -> bool:
        """Remove a book from disk cache. True if deleted, False if not found."""
        import shutil

        barcode = str(barcode)
        barcode_dir = self._cache_dir / barcode

        if not barcode_dir.exists():
            return False

        shutil.rmtree(barcode_dir)
        logger.debug(f"Deleted cached book {barcode}")
        return True
