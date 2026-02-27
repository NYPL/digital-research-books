"""V2 Pipeline - Modular data loading and indexing.

This package provides a source/transform/sink architecture for:
- Loading books from S3 or local disk
- Chunking text with configurable strategies
- Generating embeddings with caching
- Indexing to Elasticsearch (or other backends)

Example:
    from lib.v2 import (
        GlobalConfig, get_config,
        Book, BookMetadata, ChunkDocument,
        ElasticsearchBackend,
        SentenceSplitterChunker,
    )

    # Configuration is automatic (reads VRA_ENV)
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
    PROJECT_ROOT,
    CONFIG_DIR,
    DATA_DIR,
)

from vector_indexing.components import (
    # Backends
    IndexBackend,
    ElasticsearchBackend,
    DEFAULT_VECTOR_MAPPING,
)

from vector_indexing.components.chunkers import (
    TextChunker,
    SentenceSplitterChunker,
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
