"""V2 Pipeline Components.

Reusable building blocks for pipelines and other uses:
- backends: Index backend implementations (Elasticsearch, etc.)
- loaders: Book loading from S3/disk
- chunking: Text chunking strategies (Phase 4)
- embeddings: Embedding providers with caching (Phase 5)
"""

from vector_indexing.components.backends import (
    IndexBackend,
    ElasticsearchBackend,
    build_index_mapping,
    DEFAULT_VECTOR_MAPPING,  # Deprecated
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

__all__ = [
    # Backends
    "IndexBackend",
    "ElasticsearchBackend",
    "build_index_mapping",
    "DEFAULT_VECTOR_MAPPING",  # Deprecated
    # Loaders
    "BookLoader",
    "BookCache",
    "BookNotFoundError",
    "BookLoadError",
    "LocalBookLoader",
    "DiskBookCache",
    "S3BookLoader",
    "CachedS3BookLoader",
]
