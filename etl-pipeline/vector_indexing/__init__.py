"""Vector Indexing Pipeline - Modular data loading and indexing.

This package provides a source/transform/sink architecture for:
- Loading books from S3 or local disk
- Chunking text with configurable strategies
- Generating embeddings with caching
- Indexing to Elasticsearch (or other backends)

Example:
    from vector_indexing import (
        GlobalConfig, get_config,
        Book, BookMetadata, ChunkDocument,
        ElasticsearchBackend,
        SentenceSplitterChunker,
        Pipeline,
    )

    # Configuration is automatic (reads env vars)
    cfg = get_config()

    # Or explicit
    cfg = GlobalConfig(s3_bucket="my-bucket", es_index="my-index")

    # Create a backend
    backend = ElasticsearchBackend.from_config()
"""

from vector_indexing.core import (
    # Types
    Book,
    BookMetadata,
    ChunkDocument,
    InsertResult,
    # Config
    GlobalConfig,
    get_config,
    set_config,
    reset_config,
)

from vector_indexing.components import (
    # Backends
    IndexBackend,
    ElasticsearchBackend,
    DEFAULT_VECTOR_MAPPING,
    # Loaders
    BookLoader,
    BookCache,
    BookNotFoundError,
    BookLoadError,
    LocalBookLoader,
    DiskBookCache,
    S3BookLoader,
    CachedS3BookLoader,
    # Chunkers
    TextChunker,
    SentenceSplitterChunker,
    # Embedders
    Embedder,
    GoogleEmbedder,
    QwenEmbedder,
    # Metadata
    MetadataProvider,
)

from vector_indexing.pipeline import (
    Pipeline,
    IndexingResult,
    BatchResult,
)

__all__ = [
    # Types
    "Book",
    "BookMetadata",
    "ChunkDocument",
    "InsertResult",
    # Config
    "GlobalConfig",
    "get_config",
    "set_config",
    "reset_config",
    # Backends
    "IndexBackend",
    "ElasticsearchBackend",
    "DEFAULT_VECTOR_MAPPING",
    # Loaders
    "BookLoader",
    "BookCache",
    "BookNotFoundError",
    "BookLoadError",
    "LocalBookLoader",
    "DiskBookCache",
    "S3BookLoader",
    "CachedS3BookLoader",
    # Chunkers
    "TextChunker",
    "SentenceSplitterChunker",
    # Embedders
    "Embedder",
    "GoogleEmbedder",
    "QwenEmbedder",
    # Metadata
    "MetadataProvider",
    # Pipeline
    "Pipeline",
    "IndexingResult",
    "BatchResult",
]
