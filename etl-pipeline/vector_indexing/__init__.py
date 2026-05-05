"""V2 Pipeline - Modular data loading and indexing.

This package provides a source/transform/sink architecture for:
- Loading books from S3 or local disk
- Chunking text with configurable strategies
- Generating embeddings with caching
- Indexing to Elasticsearch (or other backends)

Example:
    from vector_indexing import (
        PostgresConfig, ElasticsearchConfig, QwenConfig,
        Book, BookMetadata, ChunkDocument,
        ElasticsearchBackend,
        SentenceSplitterChunker,
    )

    # Create a backend using env-var defaults
    backend = ElasticsearchBackend("my-index")

    # Or with explicit config
    es_config = ElasticsearchConfig(host="prod-es.example.com", port=9243)
    backend = ElasticsearchBackend("my-index", es_config=es_config)
"""

from vector_indexing.components import (
    DEFAULT_VECTOR_MAPPING,
    ElasticsearchBackend,
    # Backends
    IndexBackend,
)
from vector_indexing.components.chunkers import (
    SentenceSplitterChunker,
    TextChunker,
)
from vector_indexing.core import (
    CONFIG_DIR,
    DATA_DIR,
    PROJECT_ROOT,
    # Types
    Book,
    BookMetadata,
    ChunkDocument,
    ElasticsearchConfig,
    InsertResult,
    # Config
    PostgresConfig,
    QwenConfig,
    resolve_path,
)

__all__ = [
    # Types
    "Book",
    "BookMetadata",
    "ChunkDocument",
    "InsertResult",
    # Config
    "PostgresConfig",
    "ElasticsearchConfig",
    "QwenConfig",
    "resolve_path",
    "PROJECT_ROOT",
    "CONFIG_DIR",
    "DATA_DIR",
    # Backends
    "IndexBackend",
    "ElasticsearchBackend",
    "DEFAULT_VECTOR_MAPPING",
    # Chunkers
    "TextChunker",
    "SentenceSplitterChunker",
]
