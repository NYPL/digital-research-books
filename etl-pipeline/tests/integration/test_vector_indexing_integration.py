"""Integration tests for vector indexing end-to-end pipeline flows.

These tests exercise the full pipeline with real implementations
(but mocked external services like ES and S3).
"""

import pytest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from vector_indexing.core import (
    Book,
    BookMetadata,
    ChunkDocument,
    InsertResult,
    GlobalConfig,
)
from vector_indexing.components.chunkers import SentenceSplitterChunker
from vector_indexing.components.loaders import LocalBookLoader
from vector_indexing.components.backends.elasticsearch import ElasticsearchBackend
from vector_indexing.pipeline.orchestrator import Pipeline


class TestLocalToElasticsearchFlow:
    """Test loading from local files and indexing to ES."""

    @pytest.fixture
    def book_dir(self):
        """Create temp directory with sample book files."""
        with TemporaryDirectory() as tmpdir:
            # Create books/ subdirectory - this is where resolved_book_cache_dir will point
            books_path = Path(tmpdir) / "books"
            books_path.mkdir()

            # Barcode directories go directly under books/
            book_path = books_path / "33433001234567"
            book_path.mkdir()

            # Create page files
            for i in range(1, 4):
                page_file = book_path / f"page_{i:04d}.txt"
                page_file.write_text(
                    f"This is page {i} of the test book. " * 20
                    + "It contains enough text to create multiple chunks. " * 10
                )

            yield tmpdir

    @pytest.fixture
    def config(self, book_dir):
        """Config pointing to temp book directory."""
        # book_cache_dir should point to the books/ subdirectory directly
        # since LocalBookLoader expects {cache_dir}/{barcode}/
        return GlobalConfig(
            data_dir=Path(book_dir),
            book_cache_dir=Path(book_dir) / "books",
            chunk_size=200,
            chunk_overlap=20,
        )

    def test_local_load_chunk_and_embed(self, config, book_dir):
        """Load book from disk, chunk it, and verify chunk properties."""
        loader = LocalBookLoader(config=config)
        chunker = SentenceSplitterChunker(config=config)

        # Load
        book = loader.load("33433001234567")
        assert book is not None
        assert book.barcode == "33433001234567"
        assert book.page_count == 3

        # Add metadata (normally from DB)
        book = Book(
            barcode=book.barcode,
            pages=book.pages,
            book_id=book.book_id,
            metadata=BookMetadata(
                edition_id=123,
                title="Integration Test Book",
                author=["Test Author"],
                subject=["Testing"],
                publication_date="2024-01-01",
                language=["English"],
            ),
        )

        # Chunk
        chunks = list(chunker.chunk(book))

        assert len(chunks) >= 3  # Should have multiple chunks

        # Verify chunk structure
        for i, chunk in enumerate(chunks):
            assert chunk.barcode == "33433001234567"
            assert chunk.chunk_index == i
            assert chunk.doc_id == f"33433001234567_{i}"
            assert chunk.text  # Non-empty
            assert chunk.book_metadata.title == "Integration Test Book"

    def test_full_pipeline_with_mocked_es(self, config, book_dir):
        """Run full pipeline with real loader/chunker but mocked ES."""
        # Set up mocks
        mock_es_client = Mock()
        mock_es_client.indices.exists.return_value = True

        mock_embedder = Mock()
        mock_embedder.embed_batch.side_effect = lambda texts: [[0.1] * 768] * len(texts)

        mock_metadata = Mock()
        mock_metadata.get_metadata.return_value = {
            "33433001234567": BookMetadata(
                edition_id=123,
                title="Test Book",
                author=["Test Author"],
                subject=["Test"],
                publication_date="2024",
                language=["en"],
            )
        }

        with patch(
            "vector_indexing.components.backends.elasticsearch.bulk"
        ) as mock_bulk:
            mock_bulk.return_value = (10, [])  # Success

            pipeline = Pipeline(
                loader=LocalBookLoader(config=config),
                chunker=SentenceSplitterChunker(config=config),
                embedder=mock_embedder,
                metadata_provider=mock_metadata,
                backend=ElasticsearchBackend(
                    client=mock_es_client,
                    index_name="test-index",
                ),
            )

            # Make bulk return correct count - capture how many docs are passed
            def bulk_side_effect(client, actions, **kwargs):
                action_list = list(actions)
                return (len(action_list), [])

            mock_bulk.side_effect = bulk_side_effect

            result = pipeline.index_book("33433001234567")

            assert result.success
            assert result.chunks_created > 0
            assert result.chunks_inserted == result.chunks_created

            # Verify embedder was called with chunk texts
            mock_embedder.embed_batch.assert_called_once()

            # Verify bulk was called
            mock_bulk.assert_called_once()


class TestMultiBookBatch:
    """Test processing multiple books in a batch."""

    def test_batch_continues_on_failure(self):
        """Batch processing continues when one book fails."""
        meta1 = BookMetadata(
            edition_id=1,
            title="G1",
            author=[],
            subject=[],
            publication_date="2024",
            language=[],
        )
        meta2 = BookMetadata(
            edition_id=2,
            title="G2",
            author=[],
            subject=[],
            publication_date="2024",
            language=[],
        )

        # Create mocks
        loader = Mock()
        loader.load.side_effect = [
            Book(barcode="good1", pages=["text"], book_id="rec1", metadata=meta1),
            None,  # Book not found
            Book(barcode="good2", pages=["text"], book_id="rec2", metadata=meta2),
        ]

        chunker = Mock()

        def make_chunks(book):
            """Chunker now receives book with metadata already attached."""
            return [
                ChunkDocument.create(
                    barcode=book.barcode,
                    book_id=book.book_id,
                    chunk_index=0,
                    text="chunk text",
                    start_page=1,
                    end_page=1,
                    book_metadata=book.metadata,
                )
            ]

        chunker.chunk.side_effect = make_chunks

        embedder = Mock()
        embedder.embed_batch.return_value = [[0.1] * 768, [0.1] * 768]

        metadata_provider = Mock()
        metadata_provider.get_metadata.return_value = {
            "rec1": meta1,
            "rec2": meta2,
        }

        backend = Mock()
        backend.insert.return_value = InsertResult(inserted=1, failed=0)

        pipeline = Pipeline(
            loader=loader,
            chunker=chunker,
            embedder=embedder,
            metadata_provider=metadata_provider,
            backend=backend,
        )

        result = pipeline.index_books(["good1", "missing", "good2"])

        assert result.total == 3
        assert result.succeeded == 2
        assert result.failed == 1

        # Check individual results
        failures = [r for r in result.results if not r.success]
        assert len(failures) == 1
        assert failures[0].barcode == "missing"


class TestChunkDocumentElasticsearchRoundtrip:
    """Test that chunks survive the full ES index/retrieve cycle."""

    def test_chunk_survives_es_format_conversion(self):
        """ChunkDocument -> ES action -> ES hit -> ChunkDocument."""
        from vector_indexing.components.backends.elasticsearch import (
            chunk_to_es_action,
            chunk_from_es_hit,
        )

        original = ChunkDocument.create(
            barcode="33433001234567",
            book_id="rec_001",
            chunk_index=42,
            text="The quick brown fox jumps over the lazy dog.",
            start_page=10,
            end_page=12,
            book_metadata=BookMetadata(
                edition_id=99999,
                title="Fox Stories",
                author=["Aesop"],
                subject=["Animals", "Fiction"],
                publication_date="1990-05-15",
                language=["English", "Spanish"],
            ),
            vector=[0.123] * 768,
        )

        # Convert to ES action format
        action = chunk_to_es_action(original)

        # Simulate what ES returns (the action has _id and _source at top level)
        hit = {
            "_id": action["_id"],
            "_source": action["_source"],
        }

        # Convert back
        restored = chunk_from_es_hit(hit)

        # Verify key fields match
        assert restored.doc_id == original.doc_id
        assert restored.text == original.text
        assert restored.book_metadata.edition_id == original.book_metadata.edition_id
        assert restored.book_metadata.title == original.book_metadata.title
