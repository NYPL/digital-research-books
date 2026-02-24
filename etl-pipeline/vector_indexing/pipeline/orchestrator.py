"""Pipeline orchestrator for the full indexing workflow."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, TYPE_CHECKING

from vector_indexing.core.types import Book, BookMetadata, ChunkDocument

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from vector_indexing.components.backends.base import IndexBackend
    from vector_indexing.components.chunkers.base import TextChunker
    from vector_indexing.components.embedders.base import Embedder
    from vector_indexing.components.loaders.base import BookLoader
    from vector_indexing.components.metadata.provider import MetadataProvider


@dataclass
class IndexingResult:
    """Result of indexing a single book."""

    barcode: str
    book_id: str | None
    success: bool
    chunks_created: int
    chunks_inserted: int
    error: str | None = None

    def __repr__(self) -> str:
        status = "y" if self.success else "n"
        return f"IndexingResult({status} {self.barcode}, {self.chunks_inserted}/{self.chunks_created} chunks)"


@dataclass
class BatchResult:
    """Result of indexing a batch of books."""

    results: list[IndexingResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.success)

    @property
    def total_chunks_created(self) -> int:
        return sum(r.chunks_created for r in self.results)

    @property
    def total_chunks_inserted(self) -> int:
        return sum(r.chunks_inserted for r in self.results)

    def __repr__(self) -> str:
        return (
            f"BatchResult({self.succeeded}/{self.total} books, "
            f"{self.total_chunks_inserted}/{self.total_chunks_created} chunks)"
        )


# Type alias for progress callback
ProgressCallback = Callable[[IndexingResult], None]


class Pipeline:
    """Orchestrates the full book indexing pipeline.

    Pipeline stages:
    1. Load books from source (S3, local, etc.)
    2. Fetch metadata from database
    3. Chunk text into documents
    4. Generate embeddings
    5. Insert into index backend

    Use PipelineBuilder to construct a Pipeline instance.

    NOTE: The pipeline is aggressive, in that it does not fail the entire batch if
    one book fails at any stage. Instead, it records the error for that book and
    continues processing the rest. This is to maximize throughput in large batches.
    We may want to add a catastrophic failure mode at some point where if some percentage
    of books fail we abort the entire batch.

    Example:
        >>> pipeline = Pipeline(
        ...     loader=S3BookLoader(...),
        ...     chunker=SentenceSplitterChunker(),
        ...     embedder=GoogleEmbedder(),
        ...     metadata_provider=MetadataProvider(),
        ...     backend=ElasticsearchBackend(...),
        ... )
        >>> result = pipeline.index_books(["33433001234567"])
    """

    def __init__(
        self,
        loader: "BookLoader",
        chunker: "TextChunker",
        embedder: "Embedder",
        metadata_provider: "MetadataProvider",
        backend: "IndexBackend",
    ):
        self._loader = loader
        self._chunker = chunker
        self._embedder = embedder
        self._metadata_provider = metadata_provider
        self._backend = backend

    def index_book(self, barcode: str) -> IndexingResult:
        """Index a single book through the full pipeline. Returns an IndexingResult with success/failure details."""
        results = self.index_books([barcode])
        return results.results[0]

    def index_books(
        self,
        barcodes: list[str],
        on_progress: ProgressCallback | None = None,
    ) -> BatchResult:
        """Index multiple books with batched operations.
        Attempts to optimize performance by:
        - Fetching metadata for all books in one DB query
        - Embedding all chunks together in batched API calls

        The on_progress callback, if provided, is called after each book is processed.
        NOTE: we may need to reconsider the on_progress design here since it only begins
        being called at the final step.

        Returns a BatchResult containing individual results for each book.
        """
        batch_result = BatchResult()

        if not barcodes:
            return batch_result

        # Stage 1: Load all books
        books: dict[str, Book] = {}
        book_errors: dict[str, str] = {}

        for barcode in barcodes:
            try:
                book = self._loader.load(barcode)
                if book is None:
                    book_errors[barcode] = "Book not found"
                else:
                    books[barcode] = book
            except Exception as e:
                book_errors[barcode] = f"Load error: {e}"

        logger.info(f"Stage 1 (Load): {len(books)} loaded, {len(book_errors)} errors")

        # Stage 2: Fetch metadata for all books in one query (keyed by barcode)
        loaded_barcodes = list(books.keys())
        metadata_map: dict[str, BookMetadata] = {}
        if loaded_barcodes:
            try:
                metadata_map = self._metadata_provider.get_metadata(loaded_barcodes)
            except Exception:
                # If metadata fetch fails, continue with empty metadata
                # Log this in production
                pass

        logger.info(
            f"Stage 2 (Metadata): {len(metadata_map)} fetched, {len(book_errors)} errors"
        )

        # Stage 3: Chunk all books
        all_chunks: list[ChunkDocument] = []
        chunks_by_barcode: dict[str, list[ChunkDocument]] = {}

        for barcode, book in list(books.items()):
            try:
                # Get metadata or create empty default (now keyed by barcode)
                metadata = metadata_map.get(barcode) or BookMetadata(
                    edition_id=None,
                    title=None,
                    author=[],
                    subject=[],
                    publication_date=None,
                    language=[],
                )

                # Enrich book with metadata before chunking
                enriched_book = Book(
                    barcode=book.barcode,
                    pages=book.pages,
                    book_id=book.book_id,
                    metadata=metadata,
                )

                chunks = list(self._chunker.chunk(enriched_book))
                chunks_by_barcode[barcode] = chunks
                all_chunks.extend(chunks)
            except Exception as e:
                book_errors[barcode] = f"Chunk error: {e}"

        logger.info(
            f"Stage 3 (Chunk): {len(all_chunks)} chunks from {len(chunks_by_barcode)} books, {len(book_errors)} errors"
        )

        # Stage 4: Embed all chunks together (most efficient)
        if all_chunks:
            try:
                texts = [c.text for c in all_chunks]
                vectors = self._embedder.embed_batch(texts)
                for chunk, vector in zip(all_chunks, vectors):
                    chunk.vector = vector
            except Exception as e:
                # If embedding fails entirely, mark all remaining books as failed
                for barcode in chunks_by_barcode:
                    if barcode not in book_errors:
                        book_errors[barcode] = f"Embed error: {e}"
                chunks_by_barcode.clear()

        logger.info(
            f"Stage 4 (Embed): {len(all_chunks)} embedded, {len(book_errors)} errors"
        )
        # Stage 5: Insert chunks per book (to track per-book results)
        for barcode, chunks in chunks_by_barcode.items():
            book = books[barcode]
            try:
                insert_result = self._backend.insert(chunks)

                result = IndexingResult(
                    barcode=barcode,
                    book_id=book.book_id,
                    success=insert_result.failed == 0,
                    chunks_created=len(chunks),
                    chunks_inserted=insert_result.inserted,
                    error=insert_result.errors[0] if insert_result.errors else None,
                )
            except Exception as e:
                result = IndexingResult(
                    barcode=barcode,
                    book_id=book.book_id,
                    success=False,
                    chunks_created=len(chunks),
                    chunks_inserted=0,
                    error=f"Insert error: {e}",
                )

            batch_result.results.append(result)
            if on_progress:
                on_progress(result)

        # Add results for books that failed earlier
        for barcode, error in book_errors.items():
            book = books.get(barcode)
            result = IndexingResult(
                barcode=barcode,
                book_id=book.book_id if book else None,
                success=False,
                chunks_created=0,
                chunks_inserted=0,
                error=error,
            )
            batch_result.results.append(result)
            if on_progress:
                on_progress(result)

        return batch_result
