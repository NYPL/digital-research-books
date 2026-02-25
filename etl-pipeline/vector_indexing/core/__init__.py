"""Core types and configuration for vector indexing pipeline."""

from vector_indexing.core.types import (
    Book,
    BookMetadata,
    ChunkDocument,
    InsertResult,
)
from vector_indexing.core.config import (
    GlobalConfig,
    get_config,
    set_config,
    reset_config,
    PROJECT_ROOT,
    VECTOR_INDEXING_ROOT,
    CONFIG_DIR,
    DATA_DIR,
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
    "VECTOR_INDEXING_ROOT",
    "CONFIG_DIR",
    "DATA_DIR",
]
