import dataclasses
from datetime import datetime, timezone
import io
import json

from . import marc
from . import mets_parser
from . import path
import pathlib
from managers import S3Manager
from digital_assets import get_stored_file_url


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
    storage_manager: S3Manager,
    bucket_name: str,
    mets_path: path.METSPath,
    mets_file: mets_parser.METSFile,
) -> Metadata | None:
    metadata_key = mets_path.get_metadata_key(mets_file.metadata_file)

    try:
        metadata_file = mets_parser.MetadataFile.from_mets_str(
            storage_manager.get_object(key=metadata_key, bucket=bucket_name)[
                "Body"
            ].read(),
        )
    except TypeError:
        metadata_file = mets_parser.MetadataFile(mets_file.root)

    try:
        root_metadata_file = mets_parser.MetadataFile.from_mets_str(
            storage_manager.get_object(key=mets_path.mets_key, bucket=bucket_name)[
                "Body"
            ].read(),
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
