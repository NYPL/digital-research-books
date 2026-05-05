"""Pipeline orchestrator for the full indexing workflow."""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from logger import create_log

from vector_indexing.core.types import Book, BookMetadata, ChunkDocument
from vector_indexing.core.utils import Timer

logger = create_log(__name__)

if TYPE_CHECKING:
    from vector_indexing.components.backends.base import IndexBackend
    from vector_indexing.components.chunkers.base import TextChunker
    from vector_indexing.components.embedders.base import Embedder
    from vector_indexing.components.loaders.base import BookLoader
    from vector_indexing.components.metadata.provider import MetadataProvider

# These are imported at runtime for default initialization
from vector_indexing import get_config, SentenceSplitterChunker
from vector_indexing.components.loaders import S3BookLoader
from vector_indexing.components.embedders import GoogleEmbedder
from vector_indexing.components.metadata import MetadataProvider as MetadataProviderImpl
from vector_indexing.components.backends.turbopuffer import TurbopufferBackend


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

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "IndexingResult":
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})


@dataclass
class BatchResult:
    """Result of indexing a batch of books."""

    results: list[IndexingResult] = field(default_factory=list)
    total_time: float | None = None

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
        time_str = (
            f", {timedelta(seconds=self.total_time)}"
            if self.total_time is not None
            else ""
        )
        return (
            f"BatchResult({self.succeeded}/{self.total} books, "
            f"{self.total_chunks_inserted}/{self.total_chunks_created} chunks{time_str})"
        )

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "BatchResult":
        results = [IndexingResult.from_dict(r) for r in data.get("results", [])]
        return cls(results=results, total_time=data.get("total_time"))

    def save(self, save_dir: str | Path | None = None) -> Path:
        """Serialize to JSON and write to save_dir (default: CWD). Returns the saved file path."""
        save_dir = Path(save_dir) if save_dir is not None else Path.cwd()
        save_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = save_dir / f"batch_result_{timestamp}.json"
        path.write_text(json.dumps(self.to_dict(), indent=2))
        return path

    @classmethod
    def load(cls, path: str | Path) -> "BatchResult":
        """Load a BatchResult from a JSON file saved by .save()."""
        return cls.from_dict(json.loads(Path(path).read_text()))


# Type alias for progress callback
ProgressCallback = Callable[[IndexingResult], None]


def _default_on_progress(result: IndexingResult) -> None:
    """Default progress callback that prints indexing results."""
    status = "y" if result.success else "n"
    print(
        f"  {status} {result.barcode}: {result.chunks_inserted} chunks"
        + (f" ({result.error})" if result.error else "")
    )


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
        >>> pipeline = (
        ...     Pipeline.builder()
        ...     .with_loader(S3BookLoader(...))
        ...     .with_chunker(SentenceSplitterChunker())
        ...     .with_embedder(GoogleEmbedder())
        ...     .with_metadata_provider(MetadataProvider())
        ...     .with_backend(ElasticsearchBackend(...))
        ...     .build()
        ... )
        >>> result = pipeline.index_books(["33433001234567"])
    """

    def __init__(
        self,
        loader: "BookLoader" | None = None,
        chunker: "TextChunker" | None = None,
        embedder: "Embedder" | None = None,
        metadata_provider: "MetadataProvider" | None = None,
        backend: "IndexBackend" | None = None,
    ):
        # Q: is there compelling reason to define these defaults elsewhere?
        config = get_config()

        self._loader = loader if loader is not None else S3BookLoader(config=config)
        self._chunker = (
            chunker if chunker is not None else SentenceSplitterChunker(config=config)
        )
        self._embedder = embedder if embedder is not None else GoogleEmbedder()
        self._metadata_provider = (
            metadata_provider
            if metadata_provider is not None
            else MetadataProviderImpl(config=config)
        )
        self._backend = (
            backend
            if backend is not None
            else TurbopufferBackend.from_config(
                index_name="vra-dev",
                config=config,
            )
        )

    @classmethod
    def builder(cls) -> "PipelineBuilder":
        """Create a new PipelineBuilder."""
        return PipelineBuilder()

    def index_book(self, barcode: str) -> IndexingResult:
        """Index a single book through the full pipeline. Returns an IndexingResult with success/failure details."""
        results = self.index_books([barcode])
        return results.results[0]

    # TODO: create Pipeline.batch_index_books() as a convenience wrapper for \
    # programmatic access to batching

    def index_books(
        self,
        barcodes: list[str],
        on_progress: ProgressCallback | None = _default_on_progress,
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

        with Timer("index_books") as timer:
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
                    book_errors[barcode] = (
                        f"Load error: {type(e).__module__}.{type(e).__qualname__}: {e}"
                    )

            logger.info(
                f"Stage 1 (Load): {len(books)} loaded, {len(book_errors)} errors"
            )

            # Stage 2: Fetch metadata for all books in one query (keyed by barcode)
            loaded_barcodes = list(books.keys())
            metadata_map: dict[str, BookMetadata] = {}
            enriched_books = {}
            if loaded_barcodes:
                try:
                    metadata_map = self._metadata_provider.get_metadata(loaded_barcodes)
                except Exception as e:
                    book_errors.update(
                        dict(
                            zip(
                                loaded_barcodes,
                                [
                                    f"Metadata retrieval error: {type(e).__module__}.{type(e).__qualname__}: {e}"
                                ]
                                * len(loaded_barcodes),
                            )
                        )
                    )
                else:
                    for barcode, book in books.items():
                        if barcode in metadata_map:
                            # Enrich book with metadata before chunking
                            enriched_book = Book(
                                barcode=book.barcode,
                                pages=book.pages,
                                book_id=book.book_id,
                                metadata=metadata_map[barcode],
                            )
                            enriched_books[barcode] = enriched_book
                        else:
                            book_errors[barcode] = "Metadata retrieval failed"

            logger.info(
                f"Stage 2 (Metadata): {len(metadata_map)} fetched, {len(book_errors)} errors"
            )

            # Stage 3: Chunk all books
            all_chunks: list[ChunkDocument] = []
            chunks_by_barcode: dict[str, list[ChunkDocument]] = {}
            for barcode, enriched_book in list(enriched_books.items()):
                try:
                    chunks = list(self._chunker.chunk(enriched_book))
                    chunks_by_barcode[barcode] = chunks
                    all_chunks.extend(chunks)
                except Exception as e:
                    book_errors[barcode] = (
                        f"Chunk error: {type(e).__module__}.{type(e).__qualname__}: {e}"
                    )

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
                            book_errors[barcode] = (
                                f"Embed error: {type(e).__module__}.{type(e).__qualname__}: {e}"
                            )
                    chunks_by_barcode.clear()

            logger.info(
                f"Stage 4 (Embed): {len(all_chunks)} embedded, {len(book_errors)} errors"
            )

            # Stage 5: Insert chunks per book (record per-book indexing results)
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
                    err_str = f"Insert error:  {type(e).__module__}.{type(e).__qualname__}: {e}"
                    result = IndexingResult(
                        barcode=barcode,
                        book_id=book.book_id,
                        success=False,
                        chunks_created=len(chunks),
                        chunks_inserted=0,
                        error=err_str,
                    )
                    book_errors[barcode] = err_str

                batch_result.results.append(result)
                if on_progress:
                    on_progress(result)

            logger.info(f"Stage 5 (Insert): {len(book_errors)} errors")

            # Add book indexing result for each book that failed earlier
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

        batch_result.total_time = timer.elapsed
        return batch_result


def main(barcodes: list[str] | None = None) -> BatchResult:
    """Run the indexing pipeline with default components. Takes in a list of barcodes to index.
    Returns a BatchResult with indexing outcomes.
    """
    from vector_indexing.components.backends.turbopuffer import TurbopufferBackend
    from vector_indexing.components.chunkers.sentence import SentenceSplitterChunker
    from vector_indexing.components.embedders.google import GoogleEmbedder
    from vector_indexing.components.loaders.s3 import CachedS3BookLoader
    from vector_indexing.components.metadata.provider import MetadataProvider
    from vector_indexing.core.config import GlobalConfig

    if barcodes is None:
        return

    config = GlobalConfig.for_environment()

    pipeline = Pipeline(
        loader=CachedS3BookLoader(config=config),
        chunker=SentenceSplitterChunker(config=config),
        embedder=GoogleEmbedder(
            model=config.embedding_model,
            dimensions=config.embedding_dimensions,
            batch_size=config.embedding_batch_size,
        ),
        metadata_provider=MetadataProvider(config=config),
        backend=TurbopufferBackend.from_config(
            index_name="vra-dev-test", config=config
        ),
    )

    def on_progress(result: IndexingResult) -> None:
        print(result)

    result = pipeline.index_books(barcodes, on_progress=on_progress)
    print(f"\n{result}")
    return result


if __name__ == "__main__":
    # Test using a few very small books
    # main(["33433000136972", "33433006239176"])
    # main(["33433071108306", "33433009163845"])
    pass
