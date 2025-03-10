from dataclasses import dataclass, asdict

from enum import Enum
import json
from sqlalchemy import Column, DateTime, Integer, Unicode, Boolean, Index
from sqlalchemy.dialects.postgresql import ARRAY, UUID, ENUM
from sqlalchemy.ext.hybrid import hybrid_property
from model.utilities.extractDailyEdition import extract
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

    def get_file_bucket(self) -> Optional[str]:
        parsed_url = urlparse(self.url)

        if 'localhost' in parsed_url.hostname:
            path_parts = parsed_url.path.split('/')
            
            return path_parts[1]

        if 's3' not in parsed_url.hostname:
            return None
        
        return parsed_url.hostname.split('.')[0]

    def get_file_key(self) -> Optional[str]:
        parsed_url = urlparse(self.url)

        if 'localhost' in parsed_url.hostname:
            path_parts = parsed_url.path.split('/')

            return '/'.join(path_parts[2:])

        if 's3' not in parsed_url.hostname:
            return None
        
        return parsed_url.path[1:]
    
    def to_string(self) -> str:
        return '|'.join([
            str(self.index) if self.index is not None else '', 
            self.url, 
            self.source, 
            self.file_type, 
            self.flags
        ])


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

    def to_string(self) -> str:
        return json.dumps(asdict(self))

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
    has_part = Column(ARRAY(Unicode, dimensions=1)) # dc:hasPart, Repeating, Format "itemNo|uri|source|type|flags"
    coverage = Column(ARRAY(Unicode, dimensions=1)) # dc:coverage, non-Repeating, Format "locationCode|locationName|itemNo"

    __tableargs__ = (Index('ix_record_identifiers', identifiers, postgresql_using="gin"))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._deletion_flag = False

    def __repr__(self):
        return '<Record(title={}, uuid={}, authors={})>'.format(
            self.title, self.uuid, self.authors
        )
    
    def __dir__(self):
        return ['uuid', 'frbr_status', 'cluster_status', 'source', 'publisher_project_source', 'source_id',
            'title', 'alternative', 'medium', 'is_part_of', 'subjects', 'authors',
            'contributors', 'languages', 'dates', 'rights', 'identifiers',
            'date_submitted', 'requires', 'spatial', 'publisher', 'has_version',
            'table_of_contents', 'extent', 'abstract', 'has_part', 'coverage'
        ]
    
    def __iter__(self):
        for attr in dir(self):
            yield attr, getattr(self, attr)


    def get_parts(self) -> list[Part]:
        parts = []

        for part in self.has_part:
            index, file_url, source, file_type, flags = part.split('|')

            parts.append(Part(None if index is None or index == '' else int(index), file_url, source, file_type, flags))

        return parts

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
