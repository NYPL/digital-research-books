"""Core types and configuration for vector indexing pipeline."""

from vector_indexing.core.config import (
    CONFIG_DIR,
    DATA_DIR,
    PROJECT_ROOT,
    VECTOR_INDEXING_ROOT,
    ElasticsearchConfig,
    PostgresConfig,
    QwenConfig,
    resolve_path,
)
from vector_indexing.core.types import (
    Book,
    BookMetadata,
    ChunkDocument,
    InsertResult,
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
    "VECTOR_INDEXING_ROOT",
    "CONFIG_DIR",
    "DATA_DIR",
]
