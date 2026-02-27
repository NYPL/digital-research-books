"""Abstract base class for index backends."""

from abc import ABC, abstractmethod
from typing import Iterator, Optional, TYPE_CHECKING

from vector_indexing.core.types import InsertResult

if TYPE_CHECKING:
    from vector_indexing.core.types import ChunkDocument


class IndexBackend(ABC):
    """Abstract interface for search index backends.

    Implementations include Elasticsearch, turbopuffer etc.
    All methods should be idempotent where possible to support pipeline retry semantics.
    """

    @property
    @abstractmethod
    def index_name(self) -> str:
        """Name of the index this backend operates on."""
        ...

    # -------------------------------------------------------------------------
    # Index Lifecycle
    # -------------------------------------------------------------------------

    @abstractmethod
    def exists(self) -> bool:
        """Check if the index exists. True if the index exists, False otherwise."""
        ...

    @abstractmethod
    def create(self, mappings: dict, settings: Optional[dict] = None) -> None:
        """Create the index with specified mappings and settings. mappings specify the index schema.
        settings specify index-specific settings varies by backend. Raises an exception if the index already exists
        or creation fails."""
        ...

    @abstractmethod
    def delete(self) -> None:
        """Delete the index. Raises an exception if index doesn't exist or deletion fails."""
        ...

    def ensure_exists(self, mappings: dict, settings: Optional[dict] = None) -> bool:
        """Create index if it doesn't exist. True if index was created, False if it already existed."""
        if self.exists():
            return False
        self.create(mappings, settings)
        return True

    # Document Operations

    @abstractmethod
    def delete_document(self, doc_id: str) -> bool:
        """Delete a document by ID. True if deleted, False if not found."""
        ...

    @abstractmethod
    def patch_document(self, doc_id: str, fields: dict) -> bool:
        """Partially update a document (merge fields). True if updated, False if not found."""
        ...

    @abstractmethod
    def get_existing_ids(self, candidate_ids: list[str]) -> set[str]:
        """Check which document IDs already exist in the index. Returns the set of ids that are in the index. Necessary for idempotent operations."""
        ...

    @abstractmethod
    def insert(
        self, chunks: list["ChunkDocument"], batch_size: int = 500
    ) -> InsertResult:
        """Insert ChunkDocuments into the index. This is the primary endpoint for the pipeline. Implementations should
        convert ChunkDocuments to backend-specific format. Optional batch size to process documents in bulk. Returns an InsertResult with
        counts of inserted, skipped, failed documents."""
        ...

    # Querying

    @abstractmethod
    def get_document(self, doc_id: str) -> Optional["ChunkDocument"]:
        """Get a document by ID. Returns a ChunkDocument or None if not found."""
        ...

    @abstractmethod
    def scan_all_ids(self) -> Iterator[str]:
        """Iterate over all document IDs in the index."""
        ...

    @abstractmethod
    def scan_all_documents(self) -> Iterator["ChunkDocument"]:
        """Iterate over all documents in the index."""
        ...

    @abstractmethod
    def scan(
        self,
        filters: Optional[list] = None,
        rank_by: Optional[tuple[str, str]] = None,
        top_k: int = 10_000,
    ) -> Iterator["ChunkDocument"]:
        """Scan/export documents with optional filtering and ordering.

        Args:
            filters: Backend-native filter (e.g., turbopuffer ['field', 'Op', 'value'])
            rank_by: Ordering tuple (field, 'asc'|'desc'), default ('id', 'asc')
            top_k: Page size for iteration

        Returns iterator of ChunkDocuments.
        """
        ...

    @abstractmethod
    def count(self, query: Optional[dict] = None) -> int:
        """Count documents in the index. Optional query to filter documents."""
        ...

    # Stats

    def get_stats(self) -> dict:
        """Get index statistics (document count, size, etc.)
        Can (and should) be overidden, default implementation returns basic info (index name, existence, count)."""
        return {
            "index": self.index_name,
            "exists": self.exists(),
            "count": self.count() if self.exists() else 0,
        }
