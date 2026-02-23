"""Vector Indexing Pipeline Components.

Reusable building blocks for pipelines and other uses:
- backends: Index backend implementations (Elasticsearch, etc.)
- loaders: Book loading from S3/disk
- chunkers: Text chunking strategies
- embedders: Embedding providers with caching
- metadata: Metadata enrichment from PostgreSQL
"""

from vector_indexing.components.backends import (
    IndexBackend,
    ElasticsearchBackend,
    build_index_mapping,
    DEFAULT_VECTOR_MAPPING,
)
from vector_indexing.components.loaders import (
    BookLoader,
    BookCache,
    BookNotFoundError,
    BookLoadError,
    LocalBookLoader,
    DiskBookCache,
    S3BookLoader,
    CachedS3BookLoader,
)
from vector_indexing.components.chunkers import (
    TextChunker,
    SentenceSplitterChunker,
)
from vector_indexing.components.embedders import (
    Embedder,
    GoogleEmbedder,
    QwenEmbedder,
)
from vector_indexing.components.metadata import MetadataProvider

__all__ = [
    # Backends
    "IndexBackend",
    "ElasticsearchBackend",
    "build_index_mapping",
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
]
