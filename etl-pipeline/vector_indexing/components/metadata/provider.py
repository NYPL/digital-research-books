"""Metadata provider for fetching book metadata from PostgreSQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text

from vector_indexing.core.config import PostgresConfig
from vector_indexing.core.types import BookMetadata

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class MetadataProvider:
    """Fetches book metadata from PostgreSQL database.

    Queries grin_statuses to map barcodes -> record_ids, then queries
    records, items, editions, and works tables to build complete
    BookMetadata objects.

    Takes in:
        engine: Optional SQLAlchemy engine. If not provided, creates one
            using PostgresConfig (which reads from POSTGRES_* env vars).
        pg_config: Optional PostgresConfig override.
    """

    def __init__(
        self,
        engine: "Engine | None" = None,
        pg_config: PostgresConfig | None = None,
    ):
        self._engine = engine or create_engine(
            (pg_config or PostgresConfig()).connection_url
        )

    def get_metadata(self, barcodes: list[str]) -> dict[str, BookMetadata]:
        """Fetch metadata for a batch of barcodes.
        Returns a dict mapping barcode -> BookMetadata. Barcodes not found are omitted.
        """
        if not barcodes:
            return {}

        # Step 1: Map barcodes -> record_ids via grin_statuses
        barcode_to_record_id = self._get_record_ids_for_barcodes(barcodes)
        if not barcode_to_record_id:
            return {}

        record_ids = list(barcode_to_record_id.values())

        # Step 2: Fetch record data (titles) by record_id
        record_data = self._get_record_data(record_ids)

        # Step 3: Fetch edition metadata by record_id
        edition_data = self._get_edition_metadata(record_ids)

        # Step 4: Merge and key by barcode
        result: dict[str, BookMetadata] = {}
        for barcode, record_id in barcode_to_record_id.items():
            rec = record_data.get(record_id, {})
            ed = edition_data.get(record_id, {})

            result[barcode] = BookMetadata(
                edition_id=ed.get("edition_id"),
                title=rec.get("title"),
                author=ed.get("author", []),
                subject=ed.get("subject", []),
                publication_date=ed.get("publication_date"),
                language=ed.get("language", []),
            )

        return result

    def get_metadata_single(self, barcode: str) -> BookMetadata | None:
        """Fetch metadata for a single barcode.
        Returns a BookMetadata if found, None otherwise.
        """
        result = self.get_metadata([barcode])
        return result.get(barcode)

    def _get_record_ids_for_barcodes(self, barcodes: list[str]) -> dict[str, int]:
        """Look up record_ids for barcodes via grin_statuses table.
        Returns a dict mapping barcode -> record_id (only for found barcodes).
        """
        query = text("""
            SELECT barcode, record_id
            FROM grin_statuses
            WHERE barcode = ANY(:barcodes)
              AND record_id IS NOT NULL
        """)

        with self._engine.connect() as conn:
            result = conn.execute(query, {"barcodes": barcodes})
            rows = result.fetchall()

        return {row[0]: row[1] for row in rows}

    def _get_record_data(self, record_ids: list[int]) -> dict[int, dict]:
        """Query records table for titles.
        Returns a dict mapping record_id -> {"title": str}.
        """
        if not record_ids:
            return {}

        query = text("""
            SELECT id, title
            FROM records
            WHERE id = ANY(:record_ids)
        """)

        with self._engine.connect() as conn:
            result = conn.execute(query, {"record_ids": record_ids})
            rows = result.fetchall()

        return {row[0]: {"title": row[1]} for row in rows}

    def _get_edition_metadata(self, record_ids: list[int]) -> dict[int, dict]:
        """Get editions by record_id.
        Query items -> editions -> works for edition metadata.

        Returns a dict mapping record_id -> {
            "edition_id": int,
            "language": list[str],
                "publication_date": str | None,
                "subject": list[str],
            }
        """
        if not record_ids:
            return {}

        query = text("""
            SELECT 
                items.record_id,
                editions.id as edition_id,
                editions.languages,
                editions.publication_date,
                works.subjects,
                works.authors
            FROM items
            JOIN editions ON items.edition_id = editions.id
            LEFT JOIN works ON editions.work_id = works.id
            WHERE items.record_id = ANY(:record_ids)
        """)

        with self._engine.connect() as conn:
            result = conn.execute(query, {"record_ids": record_ids})
            rows = result.fetchall()

        edition_map: dict[int, dict] = {}
        for row in rows:
            record_id_val = row[0]  # int
            edition_id = row[1]
            languages_jsonb = row[2]  # JSONB: [{"language": "English", ...}, ...]
            publication_date = row[3]  # date type
            subjects_jsonb = row[4]  # JSONB: [{"heading": "Poetry", ...}, ...]
            authors_jsonb = row[5]  # JSONB: [{"name": "Author Name", ...}, ...]

            # NOTE: The following calls require the jsonb fields'
            # inner json schema to be known. If those schemas change this will break.
            # Parse languages - extract "language" field, deduplicated
            parsed_languages = self._parse_jsonb_field(languages_jsonb, "language")
            # Parse subjects - extract "heading" field, deduplicated
            parsed_subjects = self._parse_jsonb_field(subjects_jsonb, "heading")
            # Parse authors - extract "name" field, deduplicated
            parsed_authors = self._parse_jsonb_field(authors_jsonb, "name")

            # Format publication_date as string
            pub_date_str = str(publication_date) if publication_date else None

            edition_map[record_id_val] = {
                "edition_id": edition_id,
                "language": parsed_languages,
                "publication_date": pub_date_str,
                "subject": parsed_subjects,
                "author": parsed_authors,
            }

        return edition_map

    @staticmethod
    def _parse_jsonb_field(jsonb_data: list | None, field_name: str) -> list[str]:
        """Extract a field from a list of JSONB objects, deduplicated. Takes in the list of
        dicts from the jsonb object and the key to extract from each dict. Deduplicates and preserves order.
        If key is not found will silently continue.
        """
        if not jsonb_data:
            return []

        seen: set[str] = set()
        result: list[str] = []

        for obj in jsonb_data:
            if isinstance(obj, dict):
                value = obj.get(field_name)
                if value and value not in seen:
                    result.append(value)
                    seen.add(value)

        return result
