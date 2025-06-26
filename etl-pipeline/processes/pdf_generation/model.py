import dataclasses
from datetime import datetime, timezone
import io
import json

import marc
import mets_parser
import path
import pathlib
from . import s3


@dataclasses.dataclass
class Metadata:
    author: str | None
    subject: str | None
    title: str | None
    publication_date: str | None
    publication_place: str | None
    publisher: str | None
    process_date: str | None
    isbn: str | None
    oclc_number: str | None
    source: str | None
    identifier_type: str | None
    identifier_value: str | None


def get_metadata(
    bucket: s3.Bucket,
    mets_path: path.METSPath,
    mets_file: mets_parser.METSFile,
) -> Metadata | None:
    metadata_key = mets_path.get_metadata_key(mets_file.metadata_file)

    try:
        metadata_file = mets_parser.MetadataFile.from_mets_str(
            bucket.get(key=metadata_key)["Body"].read(),
        )
    except TypeError:
        metadata_file = mets_parser.MetadataFile(mets_file.root)

    try:
        root_metadata_file = mets_parser.MetadataFile.from_mets_str(
            bucket.get(key=mets_path.mets_key)["Body"].read(),
        )
        source_identifier = root_metadata_file.get_source_identifier()
    except AttributeError:
        source_identifier = metadata_file.get_source_identifier()

    metadata = metadata_file.get_metadata()

    if metadata.md_type == "MARC":
        marc_record = marc.Record.from_node(metadata.xml_data)

        return Metadata(
            author=marc_record.author,
            subject=marc_record.subject,
            title=marc_record.title,
            publication_date=marc_record.publication_date,
            publication_place=marc_record.publication_place,
            publisher=marc_record.publisher,
            process_date=datetime.now(timezone.utc).date().isoformat(),
            isbn=marc_record.isbn,
            oclc_number=marc_record.oclc_number,
            source=source_identifier.source,
            identifier_type=source_identifier.identifier_type,
            identifier_value=source_identifier.identifier_value,
        )

    return None


def write_metadata(
    bucket: s3.Bucket,
    mets_path: path.METSPath,
    metadata: Metadata,
    key: pathlib.Path,
) -> None:
    today = datetime.now(timezone.utc).date()

    metadata_json = {
        "author": metadata.author,
        "subject": metadata.subject,
        "title": metadata.title,
        "publication_date": metadata.publication_date,
        "publication_place": metadata.publication_place,
        "publisher": metadata.publisher,
        "pdf_link": bucket.get_public_url(key),
        "process_date": datetime.now(timezone.utc).date().isoformat(),
        "oclc_number": metadata.oclc_number,
        "isbn": metadata.isbn,
        "source": metadata.source,
        "identifier_type": metadata.identifier_type,
        "identifier_value": metadata.identifier_value,
    }

    with io.BytesIO() as outstream:
        outstream.write(json.dumps(metadata_json).encode())
        outstream.seek(0)
        key = mets_path.get_metadata_file_key(today)
        bucket.upload_file(outstream, key=key)
