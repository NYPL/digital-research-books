from dataclasses import dataclass, asdict, field

from enum import Enum
import json
from sqlalchemy import Column, DateTime, Integer, Unicode, Boolean, Index
from sqlalchemy.dialects.postgresql import ARRAY, UUID, ENUM
from sqlalchemy.ext.hybrid import hybrid_property
from model.utilities.extractDailyEdition import extract
from textwrap import shorten
from typing import Optional
from urllib.parse import urlparse

from .base import Base, Core

@dataclass
class Part:
    index: Optional[int]
    url: str
    source: str
    file_type: str
    flags: str
    source_url: Optional[str] = None

    def _parse_file_bucket(self, url: Optional[str]) -> Optional[str]:
        if url is None:
            return None

        parsed_url = urlparse(url)

        if 'localhost' in parsed_url.hostname:
            path_parts = parsed_url.path.split('/')

            return path_parts[1]

        if 's3' not in parsed_url.hostname:
            return None

        return parsed_url.hostname.split('.')[0]

    @property
    def file_bucket(self) -> Optional[str]:
        return self._parse_file_bucket(self.url)

    @property
    def source_file_bucket(self) -> Optional[str]:
        return self._parse_file_bucket(self.source_url)

    def _parse_file_key(self, url: Optional[str]) -> Optional[str]:
        if url is None:
            return None

        parsed_url = urlparse(url)

        if 'localhost' in parsed_url.hostname:
            path_parts = parsed_url.path.split('/')

            return '/'.join(path_parts[2:])

        if 's3' not in parsed_url.hostname:
            return None

        return parsed_url.path[1:]

    @property
    def file_key(self) -> Optional[str]:
        return self._parse_file_key(self.url)

    @property
    def source_file_key(self) -> Optional[str]:
        return self._parse_file_key(self.source_url)

    def __str__(self):
        fields = [
            str(self.index) if self.index is not None else '',
            self.url,
            self.source,
            self.file_type,
            self.flags
        ]

        if self.source_url is not None:
            fields.append(self.source_url)

        return '|'.join(fields)


class FRBRStatus(Enum):
    TODO = 'to_do'
    COMPLETE = 'complete'


@dataclass
class FileFlags:
    catalog: bool = False
    reader: bool = False
    embed: bool = False
    download: bool = False
    cover: bool = False
    fulfill_limited_access: bool = False
    nypl_login: bool = False

    def __str__(self):
        return json.dumps({ flag_name: flag for flag_name, flag in asdict(self).items() if flag is True })


class Record(Base, Core):
    __tablename__ = 'records'
    id = Column(Integer, primary_key=True)
    uuid = Column(UUID(as_uuid=True), nullable=False, index=True)
    frbr_status = Column(
        Unicode,
        ENUM('to_do', 'in_progress', 'complete', name='status_enum', create_type=False),
        nullable=False,
        index=True
    )
    cluster_status = Column(Boolean, default=False, nullable=False, index=True)
    state = Column(
        Unicode,
        ENUM(
            "ingested", "files_saved", "complete",
            name="record_state", create_type=False,
        ),
        nullable=True,
        index=True,
    )
    source = Column(Unicode, index=True) # dc:source, Non-Repeating
    publisher_project_source = Column(Unicode, index=True) # dc:publisherProjectSource, Non-Repeating
    source_id = Column(Unicode, index=True) # dc:identifier, Non-Repeating
    title = Column(Unicode) # dc:title, Non-Repeating
    alternative = Column(ARRAY(Unicode, dimensions=1)) # dc:alternative, Repeating
    medium = Column(Unicode) # dc:medium, Non-Repeating
    is_part_of = Column(Unicode) # dc:isPartOf, Repeating, Format "string|int|type"
    subjects = Column(ARRAY(Unicode, dimensions=1)) # dc:subject, Repeating, Format "string|authority|controlno"
    authors = Column(ARRAY(Unicode, dimensions=1)) # dc:creator, Repeating, Format "string|viaf|lcnaf|primary"
    contributors = Column(ARRAY(Unicode, dimensions=1)) # dc:contributor, Repeating, Format "string|viaf|lcnaf|role"
    languages = Column(ARRAY(Unicode, dimensions=1)) # dc:language, Repeating, Format "string|iso_2|iso_3"
    dates = Column(ARRAY(Unicode, dimensions=1)) # dc:date, Repeating, Format "string|type"
    rights = Column(Unicode) # dc:rights, Non-Repeating, Format "source|license|reason|statement|date"
    identifiers = Column(ARRAY(Unicode, dimensions=1)) # dc:identifier, Format "string|authority"
    date_submitted = Column(DateTime) # dc:dateSubmitted, Non-Repeating
    requires = Column(ARRAY(Unicode, dimensions=1)) # dc:requires, Repeating, Format "value|type"
    spatial = Column(Unicode) # dc:spatial, Non-Repeating
    publisher = Column(ARRAY(Unicode, dimensions=1)) # dc:publisher, Repeating, Format "name|viaf|lcnaf"
    _has_version = Column('has_version',Unicode) # dc:hasVersion, Non-Repeating, Format "string|edition_no"
    table_of_contents = Column(Unicode) # dc:tableOfContents, Non-Repeating
    extent = Column(Unicode) # dc:extent, Non-Repeating
    abstract = Column(Unicode) # dc:abstract, Non-Repeating
    has_part = Column(ARRAY(Unicode, dimensions=1)) # dc:hasPart, Repeating, Format "itemNo|uri|source|type|flags" or "itemNo|uri|source|type|flags|sourceUri" if the file was stored
    coverage = Column(ARRAY(Unicode, dimensions=1)) # dc:coverage, non-Repeating, Format "locationCode|locationName|itemNo"

    __tableargs__ = (Index('ix_record_identifiers', identifiers, postgresql_using="gin"))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._deletion_flag = False

    # Because SQL alchemy uses __dict__ and vars, we must use a separate to_dict function to make a Record JSON serializable
    def to_dict(self):
        return { attribute: value for attribute, value in self }

    def __repr__(self):
        title = shorten(self.title, width=50, placeholder='...') if self.title else self.title

        return f"<Record(title={title}, source={self.source} uuid={self.uuid})>"

    def __dir__(self):
        return ['uuid', 'frbr_status', 'cluster_status', 'source', 'publisher_project_source', 'source_id',
            'title', 'alternative', 'medium', 'is_part_of', 'subjects', 'authors',
            'contributors', 'languages', 'dates', 'rights', 'identifiers',
            'date_submitted', 'requires', 'spatial', 'publisher', 'has_version',
            'table_of_contents', 'extent', 'abstract', 'has_part', 'coverage', 'date_modified'
        ]

    def __iter__(self):
        for attr in dir(self):
            yield attr, getattr(self, attr)

    @staticmethod
    def parse_parts(has_part: str) -> list[Part]:
        parts = []

        if not has_part:
            return parts

        for part in has_part:
            fields = part.split('|')

            if len(fields) not in (5, 6):
                continue

            index = None if fields[0] is None or fields[0] == '' else int(fields[0])
            file_url, source, file_type, flags = fields[1:5]
            source_url = fields[5] if len(fields) == 6 else None

            parts.append(Part(index, file_url, source, file_type, flags, source_url))

        return parts

    @property
    def parts(self) -> list[Part]:
        return self.parse_parts(self.has_part)

    @hybrid_property
    def has_version(self):
        return self._has_version

    @has_version.setter
    def has_version(self, versionNum):
        if versionNum is None:
            self._has_version = versionNum
        elif self.languages != [] and self.languages != None:
            editionNo = extract(versionNum, self.languages[0].split('|')[0])
            self._has_version = f'{versionNum}|{editionNo}'
        else:
            editionNo = extract(versionNum, 'english')
            self._has_version = f'{versionNum}|{editionNo}'

    @property
    def deletion_flag(self):
        return self._deletion_flag

    @deletion_flag.setter
    def deletion_flag(self, deletion_flag):
        self._deletion_flag = deletion_flag
