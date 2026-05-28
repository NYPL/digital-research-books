"""Elasticsearch backend implementation."""

from typing import Iterator, Optional

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.helpers import scan as es_scan
from logger import create_log

from vector_indexing.components.backends.base import IndexBackend
from vector_indexing.core.config import ElasticsearchConfig
from vector_indexing.core.types import BookMetadata, ChunkDocument, InsertResult
from vector_indexing.core.utils import format_bytes

logger = create_log(__name__)


def load_default_index_mapping(
    vector_dims: int = 768,
    vector_index_type: str = "bbq_hnsw",
) -> dict[str, dict]:
    """Build Elasticsearch mapping for book chunk index."""
    return {
        # Book metadata
        "edition_id": {"type": "long"},
        "title": {"type": "text"},
        "subject": {"type": "text"},
        "language": {"type": "text"},
        #  Chunk metadata
        "text": {"type": "text"},
        "book_id": {"type": "keyword"},
        "chunk_start_page": {"type": "integer"},
        "chunk_end_page": {"type": "integer"},
        # Vector embedding
        "embedding": {
            "type": "dense_vector",
            "dims": vector_dims,
            "index_options": {
                "type": vector_index_type,
            },
        },
    }


DEFAULT_VECTOR_MAPPING = load_default_index_mapping(vector_dims=768)


# ES-specific serialization helpers


def chunk_to_es_action(chunk: ChunkDocument) -> dict:
    """Convert ChunkDocument to ES bulk action format. Takes in a ChunkDocument and returns
    a dict with "_id" and "_source" keys for ES bulk indexing.
    """
    source = {
        "text": chunk.text,
        "book_id": chunk.book_id,
        "chunk_start_page": chunk.start_page,
        "chunk_end_page": chunk.end_page,
        "edition_id": chunk.book_metadata.edition_id,
        "title": chunk.book_metadata.title,
        "author": chunk.book_metadata.author,
        "subject": chunk.book_metadata.subject,
        "publication_date": chunk.book_metadata.publication_date,
        "language": chunk.book_metadata.language,
    }
    if chunk.vector is not None:
        source["embedding"] = chunk.vector

    return {
        "_id": chunk.doc_id,
        "_source": source,
    }


def chunk_from_es_hit(hit: dict) -> ChunkDocument:
    """Reconstruct ChunkDocument from ES hit/document format. Takes in a dict representing an ES document
    (with "_id" and "_source" keys) and returns a ChunkDocument instance.
    """
    # Handle both {"_id": ..., "_source": ...} and flattened formats
    if "_source" in hit:
        doc_id = hit["_id"]
        source = hit["_source"]
    else:
        doc_id = hit.get("doc_id") or hit.get("_id")
        source = hit

    parts = doc_id.rsplit("_", 1)
    barcode = parts[0]
    chunk_index = int(parts[1]) if len(parts) > 1 else 0

    # Build BookMetadata
    book_metadata = BookMetadata(
        edition_id=source.get("edition_id"),
        title=source.get("title"),
        author=source.get("author", []),
        subject=source.get("subject", []),
        publication_date=source.get("publication_date"),
        language=source.get("language", []),
    )

    return ChunkDocument(
        doc_id=doc_id,
        text=source.get("text", ""),
        barcode=barcode,
        book_id=source.get("book_id", barcode),
        chunk_index=chunk_index,
        start_page=source.get("chunk_start_page", 1),
        end_page=source.get("chunk_end_page", 1),
        book_metadata=book_metadata,
        vector=source.get("embedding"),
    )


class ElasticsearchBackend(IndexBackend):
    """Elasticsearch implementation of IndexBackend.
    Examples:
        # Using default config (reads VRA_ELASTICSEARCH_* env vars)
        backend = ElasticsearchBackend("index-name")

        # Using explicit config
        es_config = ElasticsearchConfig(host="prod-es.example.com", port=9243)
        backend = ElasticsearchBackend("index-name", es_config=es_config)

        # Using existing client
        client = Elasticsearch(<host>)
        backend = ElasticsearchBackend("index-name", client=client)
    """

    def __init__(
        self,
        index_name: str,
        client: Optional[Elasticsearch] = None,
        es_config: ElasticsearchConfig | None = None,
    ):
        self._index_name = index_name

        if client is not None:
            self._client = client
        else:
            self._es_config = es_config or ElasticsearchConfig()
            self._client = self._create_client()

    def _create_client(self) -> Elasticsearch:
        """Create Elasticsearch client from config."""
        cfg = self._es_config
        return Elasticsearch(
            hosts=[cfg.url],
            request_timeout=cfg.timeout,
        )

    @property
    def index_name(self) -> str:
        return self._index_name

    @property
    def client(self) -> Elasticsearch:
        """Access the underlying Elasticsearch client."""
        return self._client

    # Index Lifecycle

    def exists(self) -> bool:
        return self._client.indices.exists(index=self._index_name)

    def create(self, mappings: dict, settings: Optional[dict] = None) -> None:
        body = {"mappings": {"properties": mappings}}
        if settings:
            body["settings"] = settings

        self._client.indices.create(index=self._index_name, body=body)
        logger.info(f"Created index '{self._index_name}'")

    def create_from_defaults(
        self, vector_dims: int = 768, settings: Optional[dict] = None
    ) -> None:
        mappings = load_default_index_mapping(vector_dims=vector_dims)
        self.create(mappings, settings)

    def delete(self) -> None:
        self._client.indices.delete(index=self._index_name)
        logger.info(f"Deleted index '{self._index_name}'")

    def refresh(self) -> None:
        self._client.indices.refresh(index=self._index_name)

    # Document Operations

    def insert(
        self, chunks: list[ChunkDocument], batch_size: int = 500
    ) -> InsertResult:
        """Insert ChunkDocuments into the index.
        Returns an InsertResult with counts of inserted, skipped, failed documents."""
        if not chunks:
            return InsertResult()

        # Convert ChunkDocuments to ES actions
        actions = [chunk_to_es_action(chunk) for chunk in chunks]

        inserted = 0
        failed = 0
        errors = []

        try:
            success, failed_items = bulk(
                self._client,
                actions,
                chunk_size=batch_size,
                raise_on_error=False,
                raise_on_exception=False,
            )
            inserted = success

            if isinstance(failed_items, list):
                for item in failed_items:
                    failed += 1
                    errors.append(item)
                    logger.warning(f"Bulk index error: {item}")
            elif failed_items:
                failed = failed_items

        except Exception as e:
            logger.error(f"Bulk index exception: {e}")
            errors.append({"error": str(e)})
            failed = len(chunks)

        return InsertResult(
            inserted=inserted,
            failed=failed,
            errors=errors,
        )

    def delete_document(self, doc_id: str) -> bool:
        try:
            self._client.delete(index=self._index_name, id=doc_id)
            return True
        except Exception as e:
            if "not_found" in str(e).lower():
                return False
            raise

    def patch_document(self, doc_id: str, fields: dict) -> bool:
        try:
            self._client.update(
                index=self._index_name,
                id=doc_id,
                body={"doc": fields},
            )
            return True
        except Exception as e:
            if "not_found" in str(e).lower():
                return False
            raise

    def get_existing_ids(self, candidate_ids: list[str]) -> set[str]:
        if not candidate_ids:
            return set()

        try:
            response = self._client.mget(
                index=self._index_name,
                body={"ids": candidate_ids},
                _source=False,
            )

            existing = set()
            for doc in response.get("docs", []):
                if doc.get("found", False):
                    existing.add(doc["_id"])

            return existing

        except Exception as e:
            # Fallback to scan if mget fails (e.g., too many IDs)
            logger.warning(f"mget failed, falling back to ids query: {e}")
            query = {"query": {"ids": {"values": candidate_ids}}}

            existing = set()
            for hit in es_scan(
                self._client,
                index=self._index_name,
                query=query,
                _source=False,
            ):
                existing.add(hit["_id"])

            return existing

    # Querying

    def get_document(self, doc_id: str) -> Optional[ChunkDocument]:
        try:
            response = self._client.get(index=self._index_name, id=doc_id)
            hit = {
                "_id": response["_id"],
                "_source": response["_source"],
            }
            return chunk_from_es_hit(hit)
        except Exception as e:
            if "not_found" in str(e).lower():
                return None
            raise

    def scan_all_ids(self) -> Iterator[str]:
        for hit in es_scan(
            self._client,
            index=self._index_name,
            query={"query": {"match_all": {}}},
            _source=False,
        ):
            yield hit["_id"]

    def scan_all_documents(
        self,
        include_vectors: bool = False,
        size: int = 1000,
        search_after: Optional[list] = None,
    ) -> Iterator[ChunkDocument]:
        """Scan all documents in the index using search_after for efficient pagination.

        Args:
            include_vectors: If True, includes 'embedding' field (excluded from _source by default).
            size: Number of documents per page (default: 1000, max: 10000).
            search_after: Sort values to resume from (for checkpoint support).

        Yields:
            ChunkDocument objects.
        """
        for hit in self._scan_raw(
            include_vectors=include_vectors, size=size, search_after=search_after
        ):
            yield chunk_from_es_hit(hit)

    def _scan_raw(
        self,
        include_vectors: bool = False,
        size: int = 1000,
        search_after: Optional[list] = None,
    ) -> Iterator[dict]:
        """Raw scan returning ES hit dicts with _id, _source, _sort.

        Used internally and by migration scripts that need the raw _sort cursor.
        """
        # Build source fields
        source = None
        if include_vectors:
            source = [
                "text",
                "book_id",
                "edition_id",
                "title",
                "author",
                "subject",
                "publication_date",
                "language",
                "chunk_start_page",
                "chunk_end_page",
                "embedding",
                "barcode",
                "chunk_index",
            ]

        # Use search_after pagination for efficient resumption
        current_search_after = search_after

        while True:
            body = {
                "query": {"match_all": {}},
                "sort": [{"_id": "asc"}],  # Sort by _id for resumable migrations
                "size": size,
            }
            if source:
                body["_source"] = source
            if current_search_after:
                body["search_after"] = current_search_after

            response = self._client.search(index=self._index_name, body=body)
            hits = response["hits"]["hits"]

            if not hits:
                break

            for hit in hits:
                yield {
                    "_id": hit["_id"],
                    "_source": hit["_source"],
                    "_sort": hit["sort"],
                }

            # Use last hit's sort value for next page
            current_search_after = hits[-1]["sort"]

    def scan(
        self,
        filters: Optional[dict] = None,
        rank_by: Optional[tuple[str, str]] = None,
        top_k: int = 10_000,
    ) -> Iterator[ChunkDocument]:
        """Scan documents with optional ES query filter.

        Args:
            filters: ES query dict, e.g. {"match": {"barcode": "123"}}
            rank_by: Ignored for ES (uses _doc sort)
            top_k: Page size
        """
        query = filters if filters else {"match_all": {}}

        for hit in es_scan(
            self._client,
            index=self._index_name,
            query=query,
            size=top_k,
        ):
            yield chunk_from_es_hit(hit)

    def count(self, query: Optional[dict] = None) -> int:
        body = query if query else {"query": {"match_all": {}}}
        response = self._client.count(index=self._index_name, body=body)
        return response["count"]

    # stats

    def get_stats(self) -> dict:
        base_stats = super().get_stats()

        if not self.exists():
            return base_stats

        try:
            response = self._client.indices.stats(index=self._index_name)
            index_stats = response["indices"][self._index_name]["primaries"]

            base_stats.update(
                {
                    "docs": index_stats.get("docs", {}).get("count", 0),
                    "size_bytes": index_stats.get("store", {}).get("size_in_bytes", 0),
                    "size_human": format_bytes(
                        index_stats.get("store", {}).get("size_in_bytes", 0)
                    ),
                }
            )
        except Exception as e:
            logger.warning(f"Failed to get index stats: {e}")

        return base_stats
