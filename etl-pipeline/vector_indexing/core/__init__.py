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
]
