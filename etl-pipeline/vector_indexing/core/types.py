"""Core data types for the v2 pipeline."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BookMetadata:
    """Required metadata for book indexing.

    Populated during the metadata enrichment phase from external sources
    (e.g., GRIN API, catalog records).
    """

    edition_id: int
    title: str
    author: list[str]
    subject: list[str]
    publication_date: str
    language: list[str]


@dataclass
class Book:
    """A book loaded from disk or S3."""

    barcode: str
    pages: list[str]
    book_id: str | None = None
    metadata: BookMetadata | None = None  # None until enrichment phase

    def __post_init__(self):
        # Default book_id to barcode if not provided
        if self.book_id is None:
            self.book_id = self.barcode

    @property
    def text(self) -> str:
        """Full book text with pages joined by newlines."""
        return "\n".join(self.pages)

    @property
    def page_count(self) -> int:
        return len(self.pages)

    def __repr__(self) -> str:
        return f"Book(barcode={self.barcode!r}, pages={self.page_count}, book_id={self.book_id!r})"


@dataclass
class ChunkDocument:
    """A text chunk with metadata.

    This is the primary DTO for pipeline transforms.
    """

    doc_id: str
    text: str
    barcode: str
    book_id: str
    chunk_index: int
    start_page: int
    end_page: int
    # Book metadata (required)
    book_metadata: BookMetadata
    # Optional fields
    vector: Optional[list[float]] = None

    @classmethod
    def create(
        cls,
        barcode: str,
        book_id: str,
        chunk_index: int,
        text: str,
        start_page: int,
        end_page: int,
        book_metadata: BookMetadata,
        **kwargs,
    ) -> "ChunkDocument":
        """Factory method that auto-generates doc_id."""
        doc_id = f"{barcode}_{chunk_index}"
        return cls(
            doc_id=doc_id,
            text=text,
            barcode=barcode,
            book_id=book_id,
            chunk_index=chunk_index,
            start_page=start_page,
            end_page=end_page,
            book_metadata=book_metadata,
            **kwargs,
        )

    def to_dict(self) -> dict:
        """Convert to plain dict (flattens book_metadata)."""
        result = {
            "doc_id": self.doc_id,
            "text": self.text,
            "barcode": self.barcode,
            "book_id": self.book_id,
            "chunk_index": self.chunk_index,
            "start_page": self.start_page,
            "end_page": self.end_page,
            "edition_id": self.book_metadata.edition_id,
            "title": self.book_metadata.title,
            "author": self.book_metadata.author,
            "subject": self.book_metadata.subject,
            "publication_date": self.book_metadata.publication_date,
            "language": self.book_metadata.language,
        }
        if self.vector is not None:
            result["vector"] = self.vector
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "ChunkDocument":
        """Reconstruct from dict (builds BookMetadata from flat fields)."""
        book_metadata = BookMetadata(
            edition_id=data["edition_id"],
            title=data["title"],
            author=data["author"],
            subject=data["subject"],
            publication_date=data["publication_date"],
            language=data["language"],
        )
        return cls(
            doc_id=data["doc_id"],
            text=data["text"],
            barcode=data["barcode"],
            book_id=data["book_id"],
            chunk_index=data["chunk_index"],
            start_page=data["start_page"],
            end_page=data["end_page"],
            book_metadata=book_metadata,
            vector=data.get("vector"),
        )

    def __repr__(self) -> str:
        has_vec = bool(self.vector)
        return f"ChunkDocument({self.doc_id!r}, pages={self.start_page}-{self.end_page}, has_vector={has_vec})"

    @property
    def estimated_bytes(self) -> int:
        """Estimate serialized size in bytes."""
        vector_size = len(self.vector) * 4 if self.vector else 0
        text_size = len(self.text.encode("utf-8")) if self.text else 0
        return vector_size + text_size + 500  # 500 for metadata overhead


@dataclass
class InsertResult:
    """Result of a sink insert operation."""

    inserted: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[dict] = field(default_factory=list)
    written_bytes: int = 0
    estimated_bytes: int = 0

    @property
    def total(self) -> int:
        return self.inserted + self.skipped + self.failed

    def __add__(self, other: "InsertResult") -> "InsertResult":
        """Combine two results (for aggregating batch results)."""
        return InsertResult(
            inserted=self.inserted + other.inserted,
            skipped=self.skipped + other.skipped,
            failed=self.failed + other.failed,
            errors=self.errors + other.errors,
            written_bytes=self.written_bytes + other.written_bytes,
            estimated_bytes=self.estimated_bytes + other.estimated_bytes,
        )

    def __repr__(self) -> str:
        return f"InsertResult(inserted={self.inserted}, skipped={self.skipped}, failed={self.failed}, written_bytes={self.written_bytes})"
