"""Unit tests for vector indexing pipeline components.

Focuses on behavior rather than individual property checks.
"""

import os
from unittest.mock import Mock, patch

import pytest
from vector_indexing.components.backends.elasticsearch import (
    ElasticsearchBackend,
    chunk_from_es_hit,
    chunk_to_es_action,
)
from vector_indexing.components.chunkers import SentenceSplitterChunker
from vector_indexing.core import (
    Book,
    BookMetadata,
    ChunkDocument,
    ElasticsearchConfig,
    InsertResult,
    PostgresConfig,
    QwenConfig,
)
from vector_indexing.pipeline.orchestrator import Pipeline

# Fixtures


@pytest.fixture
def sample_metadata():
    """Reusable book metadata."""
    return BookMetadata(
        edition_id=12345,
        title="Test Book",
        author=["Test Author"],
        subject=["Science", "Nature"],
        publication_date="1920-01-01",
        language=["English"],
    )


@pytest.fixture
def sample_book(sample_metadata):
    """Book with multiple pages for chunking tests."""
    return Book(
        barcode="33433001234567",
        pages=[
            "This is page one with some text. " * 10,
            "This is page two with more text. " * 10,
            "This is page three with final text. " * 10,
        ],
        book_id="rec_001",
        metadata=sample_metadata,
    )


@pytest.fixture
def sample_chunks(sample_metadata):
    """Pre-built chunks for backend tests."""
    return [
        ChunkDocument.create(
            barcode="33433001234567",
            book_id="rec_001",
            chunk_index=i,
            text=f"Chunk {i} text content",
            start_page=1,
            end_page=1,
            book_metadata=sample_metadata,
            vector=[0.1] * 768 if i % 2 == 0 else None,
        )
        for i in range(3)
    ]


# Core Types


class TestCoreTypes:
    """Test Book, ChunkDocument, InsertResult behavior."""

    def test_book_text_joins_pages(self):
        """Book.text concatenates pages with newlines."""
        book = Book(barcode="123", pages=["Page 1", "Page 2"])
        assert book.text == "Page 1\nPage 2"
        assert book.page_count == 2
        assert book.book_id == "123"  # defaults to barcode

    def test_chunk_document_roundtrip(self, sample_metadata):
        """ChunkDocument survives to_dict/from_dict."""
        chunk = ChunkDocument.create(
            barcode="123",
            book_id="rec_001",
            chunk_index=5,
            text="Sample text",
            start_page=10,
            end_page=12,
            book_metadata=sample_metadata,
            vector=[0.1, 0.2, 0.3],
        )

        data = chunk.to_dict()
        restored = ChunkDocument.from_dict(data)

        assert restored.doc_id == chunk.doc_id
        assert restored.text == chunk.text
        assert restored.vector == chunk.vector
        assert restored.book_metadata.title == sample_metadata.title

    def test_insert_result_aggregation(self):
        """InsertResults can be summed for batch operations."""
        r1 = InsertResult(inserted=10, failed=1, errors=["error1"])
        r2 = InsertResult(inserted=5, failed=2, errors=["error2"])

        combined = r1 + r2

        assert combined.inserted == 15
        assert combined.failed == 3
        assert combined.total == 18
        assert len(combined.errors) == 2


# Config


class TestConfig:
    """Test configuration loading."""

    def test_postgres_config_defaults(self):
        """PostgresConfig reads from POSTGRES_* env vars."""
        with patch.dict(
            os.environ,
            {
                "POSTGRES_HOST": "pg.example.com",
                "POSTGRES_PORT": "5432",
                "POSTGRES_USER": "user",
                "POSTGRES_PSWD": "pass",
                "POSTGRES_NAME": "mydb",
            },
        ):
            cfg = PostgresConfig()
            assert cfg.host == "pg.example.com"
            assert cfg.port == 5432
            assert (
                cfg.connection_url
                == "postgresql://user:pass@pg.example.com:5432/mydb"  # pragma: allowlist secret
            )

    def test_elasticsearch_config_defaults(self):
        """ElasticsearchConfig reads from VRA_ELASTICSEARCH_* env vars."""
        with patch.dict(
            os.environ,
            {
                "VRA_ELASTICSEARCH_HOST": "es.example.com",
                "VRA_ELASTICSEARCH_PORT": "9200",
            },
            clear=False,
        ):
            # Remove optional vars if present
            for key in (
                "VRA_ELASTICSEARCH_USER",
                "VRA_ELASTICSEARCH_PSWD",
                "VRA_ELASTICSEARCH_SCHEME",
            ):
                os.environ.pop(key, None)
            cfg = ElasticsearchConfig()
            assert cfg.host == "es.example.com"
            assert cfg.port == 9200
            assert cfg.url == "http://es.example.com:9200"

    def test_qwen_config_defaults(self):
        """QwenConfig has hardcoded defaults."""
        cfg = QwenConfig()
        assert cfg.host == "localhost"
        assert cfg.port == 1234
        assert cfg.url == "http://localhost:1234"
        assert cfg.model == "qwen3-embedding-8b-fp16"


# Chunking


class TestChunking:
    """Test text chunking behavior."""

    def test_chunker_produces_valid_documents(self, sample_book):
        """Chunker creates ChunkDocuments with all required fields."""
        chunker = SentenceSplitterChunker(chunk_size=100, chunk_overlap=10)
        chunks = list(chunker.chunk(sample_book))

        assert len(chunks) > 1  # Should produce multiple chunks

        for i, chunk in enumerate(chunks):
            assert isinstance(chunk, ChunkDocument)
            assert chunk.barcode == sample_book.barcode
            assert chunk.book_id == sample_book.book_id
            assert chunk.chunk_index == i
            assert chunk.doc_id == f"{sample_book.barcode}_{i}"
            assert chunk.text  # Has content
            assert chunk.start_page >= 1
            assert chunk.end_page >= chunk.start_page

    def test_smaller_chunk_size_produces_more_chunks(self, sample_book):
        """Smaller chunk sizes produce more chunks."""
        small = SentenceSplitterChunker(chunk_size=50, chunk_overlap=5)
        large = SentenceSplitterChunker(chunk_size=200, chunk_overlap=20)

        small_chunks = list(small.chunk(sample_book))
        large_chunks = list(large.chunk(sample_book))

        assert len(small_chunks) > len(large_chunks)


# Backend


class TestBackend:
    """Test backend operations."""

    def test_chunk_es_roundtrip(self, sample_chunks):
        """Chunks survive ES action/hit conversion."""
        for chunk in sample_chunks:
            action = chunk_to_es_action(chunk)

            # Simulate ES response format
            hit = {
                "_id": action["_id"],
                "_source": action["_source"],
            }

            restored = chunk_from_es_hit(hit)

            assert restored.doc_id == chunk.doc_id
            assert restored.text == chunk.text

    def test_elasticsearch_backend_insert(self, sample_metadata):
        """ElasticsearchBackend.insert inserts ChunkDocuments."""
        mock_client = Mock()
        mock_client.indices.exists.return_value = True

        with patch(
            "vector_indexing.components.backends.elasticsearch.bulk"
        ) as mock_bulk:
            mock_bulk.return_value = (3, [])  # 3 success, no errors

            backend = ElasticsearchBackend(
                client=mock_client,
                index_name="test-index",
            )

            chunks = [
                ChunkDocument.create(
                    barcode="123",
                    book_id="rec",
                    chunk_index=i,
                    text=f"text {i}",
                    start_page=1,
                    end_page=1,
                    book_metadata=sample_metadata,
                    vector=[0.1] * 768,
                )
                for i in range(3)
            ]

            result = backend.insert(chunks)

            assert result.inserted == 3
            assert result.failed == 0
            mock_bulk.assert_called_once()


# Pipeline


class TestPipeline:
    """Test pipeline orchestration."""

    @pytest.fixture
    def mock_pipeline(self, sample_book, sample_metadata):
        """Pipeline with all mocked components."""
        loader = Mock()
        loader.load.return_value = sample_book

        chunker = Mock()
        chunker.chunk.return_value = [
            ChunkDocument.create(
                barcode=sample_book.barcode,
                book_id=sample_book.book_id,
                chunk_index=i,
                text=f"Chunk {i}",
                start_page=1,
                end_page=1,
                book_metadata=sample_metadata,
            )
            for i in range(3)
        ]

        embedder = Mock()
        embedder.embed_batch.return_value = [[0.1] * 768] * 3

        metadata_provider = Mock()
        metadata_provider.get_metadata.return_value = {
            sample_book.barcode: sample_metadata
        }

        backend = Mock()
        backend.insert.return_value = InsertResult(inserted=3, failed=0)

        return Pipeline(
            loader=loader,
            chunker=chunker,
            embedder=embedder,
            metadata_provider=metadata_provider,
            backend=backend,
        )

    def test_pipeline_indexes_single_book(self, mock_pipeline, sample_book):
        """Pipeline processes a single book through all stages."""
        result = mock_pipeline.index_book(sample_book.barcode)

        assert result.success
        assert result.barcode == sample_book.barcode
        assert result.chunks_inserted == 3

    def test_pipeline_batches_embeddings(self, mock_pipeline, sample_book):
        """Pipeline embeds all chunks in one batch call."""
        mock_pipeline.index_books([sample_book.barcode])

        # Should call embed_batch once with all chunk texts
        mock_pipeline._embedder.embed_batch.assert_called_once()
        texts = mock_pipeline._embedder.embed_batch.call_args[0][0]
        assert len(texts) == 3

    def test_pipeline_handles_load_failure(self, mock_pipeline):
        """Pipeline gracefully handles book load failures."""
        mock_pipeline._loader.load.return_value = None

        result = mock_pipeline.index_book("missing-barcode")

        assert not result.success
        assert "not found" in result.error.lower()

    def test_pipeline_progress_callback(self, mock_pipeline, sample_book):
        """Pipeline calls progress callback after each book."""
        progress_calls = []

        mock_pipeline.index_books(
            [sample_book.barcode], on_progress=lambda r: progress_calls.append(r)
        )

        assert len(progress_calls) == 1
        assert progress_calls[0].barcode == sample_book.barcode

    def test_pipeline_skips_downstream_steps_when_metadata_fetch_fails(
        self, mock_pipeline, sample_book
    ):
        """When metadata provider raises, chunking, embedding, and insertion are skipped for that barcode."""
        mock_pipeline._metadata_provider.get_metadata.side_effect = RuntimeError(
            "DB connection failed"
        )

        result = mock_pipeline.index_book(sample_book.barcode)

        assert not result.success
        assert "Metadata retrieval error" in result.error
        mock_pipeline._chunker.chunk.assert_not_called()
        mock_pipeline._embedder.embed_batch.assert_not_called()
        mock_pipeline._backend.insert.assert_not_called()
