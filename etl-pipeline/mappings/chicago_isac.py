from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from model import Record, FRBRStatus, FileFlags, Part, Source


def map_chicago_isac_record(record: dict) -> Optional[Record]:
    isbns = get_isbns(record)
    urls = record.get("url")

    if isbns is None or urls is None or len(urls) == 0:
        return None

    pdf_part = Part(
        index=1,
        url=urls[0],
        source=Source.CHICACO_ISAC.value,
        file_type="application/pdf",
        flags=str(FileFlags(download=True)),
    )

    return Record(
        uuid=uuid4(),
        frbr_status=FRBRStatus.TODO.value,
        cluster_status=False,
        source=Source.CHICACO_ISAC.value,
        source_id=f"{Source.CHICACO_ISAC.value}_{isbns[0]}",
        title=record.get("title"),
        authors=[f"{author}|||true" for author in record.get("authors", []) if author],
        publisher=record.get("publisher"),
        identifiers=[f"{isbn}|isbn" for isbn in isbns],
        is_part_of=f"{record.get('series')}|series" if record.get("series") else None,
        spatial=record.get("publisherLocation"),
        extent=record.get("extent"),
        has_part=[str(pdf_part)],
        date_created=datetime.now(timezone.utc).replace(tzinfo=None),
        date_modified=datetime.now(timezone.utc).replace(tzinfo=None),
    )


def get_isbns(record: dict) -> Optional[list[str]]:
    isbn = record.get("isbn")

    if not isbn:
        return None

    if "," in isbn:
        return [isbn.strip() for isbn in isbn.split(",")]

    return [isbn]
