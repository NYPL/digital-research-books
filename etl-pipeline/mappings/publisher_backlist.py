from enum import Enum
from typing import Optional
import os

from digital_assets import get_stored_file_url
from .json import JSONMapping
from model import FileFlags, Part


class LimitedAccessPermissions(Enum):
    FULL_ACCESS = "Full access"
    PARTIAL_ACCESS = "Partial access/read only/no download/no login"
    LIMITED_DOWNLOADABLE = "Limited access/login for read & download"
    LIMITED_WITHOUT_DOWNLOAD = "Limited access/login for read/no download"

class PublisherBacklistMapping(JSONMapping):
    def __init__(self, source):
        super().__init__(source, {})
        self.mapping = self.createMapping()

    def createMapping(self):
        return {
            'title': ('Title', '{0}'),
            'authors': ('Author(s)', '{0}'),
            'dates': [('Pub Date', '{0}|publication_date')],
            'publisher': ('Publisher (from Project)', '{0}||'),
            'identifiers': [
                ('ISBN', '{0}|isbn'),
                ('OCLC', '{0}|oclc'),
                ('Hathi ID', '{0}|hathi')
            ],
            'rights': ('DRB Rights Classification', '{0}||||'),
            'contributors': [('Contributors', '{0}|||contributor')],
            'subjects': ('Subject 1', '{0}'),
            'source': ('Project Name (from Project)', '{0}'),
            'source_id': ('DRB_Record ID', '{0}'),
            'publisher_project_source': ('Publisher (from Project)', '{0}'),
            '_deletion_flag': ('DRB_Deleted', {0})
        }

    def applyFormatting(self):
        self.record.has_part = []
        self.add_has_part()

        if self.record.source:
            self.record.source = self.record.source[0]

        if self.record.publisher_project_source:
            publisher_source = self.record.publisher_project_source[0]
            self.record.publisher_project_source = publisher_source

        if self.record.authors:
            self.record.authors = self.format_authors()

        if self.record.subjects:
            self.record.subjects = self.format_subjects()

        if self.record.identifiers:
            self.record.identifiers = self.format_identifiers()

        self.record.rights = self.format_rights()

    def get_hathi_id(record) -> Optional[str]:
        hath_identifier = next((identifier for identifier in record.identifiers if identifier.endswith('hathi')), None)

        if hath_identifier is not None:
            return hath_identifier.split('|')[0]

        return None

    def add_has_part(self) -> str:
        record_permissions = self.parse_permissions(self.source.get('Access type in DRB (from Access types)')[0])
        file_location = self.source.get('DRB_File Location')
        destination_file_bucket = os.environ['FILE_BUCKET'] if not record_permissions['requires_login'] else f"drb-files-limited-{os.environ['ENVIRONMENT']}"
        pdf_bucket = os.environ["PDF_BUCKET"]
        hathi_id = self.get_hathi_id()

        if not file_location and not hathi_id:
            raise Exception(f'Unable to get file url for {self.record}')
        
        if hathi_id:
            self.record.has_part.append(str(Part(
                index=1,
                url=get_stored_file_url(
                    storage_name=destination_file_bucket,
                    file_path=f'titles/publisher_backlist/Schomburg/{hathi_id}/{hathi_id}.pdf',
                ),
                source=self.record.source,
                file_type='application/pdf',
                flags=str(
                    FileFlags(
                        download=record_permissions['is_downloadable'], 
                        nypl_login=record_permissions['requires_login'], 
                        fulfill_limited_access=record_permissions['requires_login']
                    )
                ),
                source_url=get_stored_file_url(
                    storage_name=pdf_bucket,
                    file_path=f'tagged_pdfs/{hathi_id}.pdf'
                )
            )))

            self.record.has_part.append(str(Part(
                url=get_stored_file_url(storage_name=os.environ['FILE_BUCKET'], file_path='covers/publisher_backlist/hathi_{hathi_id}.png'),
                source=self.record.source,
                file_type='image/png',
                flags=str(FileFlags(cover=True)),
            )))

            return

        file_path = f"titles/publisher_backlist/{self.source['Project Name (from Project)'][0]}/{self.record.source_id}.pdf"

        self.record.has_part.append(str(Part(
            index=1,
            url=get_stored_file_url(
                storage_name=destination_file_bucket,
                file_path=file_path,
            ),
            source=self.record.source,
            file_type='application/pdf',
            flags=str(
                FileFlags(
                    download=record_permissions['is_downloadable'], 
                    nypl_login=record_permissions['requires_login'], 
                    fulfill_limited_access=record_permissions['requires_login']
                )
            ),
            source_url=file_location
        )))

    def format_authors(self):
        author_list = []

        if ';' in self.record.authors:
            author_list = self.record.authors.split('; ')
            new_author_list = [f'{author}|||true' for author in author_list] 
            return new_author_list
        else:
            author_list.append(f'{self.record.authors}|||true)')
            return author_list
        
        
    def format_identifiers(self):
        if 'isbn' in self.record.identifiers[0]:
            isbn_string = self.record.identifiers[0].split('|')[0]
            isbns = []

            if ';' in isbn_string:
                isbns = isbn_string.split('; ')    
            elif ',' in isbn_string:
                isbns = isbn_string.split(', ')
 
            if isbns:
                formatted_isbns = [f'{isbn}|isbn' for isbn in isbns]
                if len(self.record.identifiers) > 1 and 'oclc' in self.record.identifiers[1]:
                    formatted_isbns.append(f'{self.record.identifiers[1]}')
                    return formatted_isbns
                else:
                    return formatted_isbns
                
        return self.record.identifiers
    
    def format_subjects(self):
        subject_list = []

        if '|' in self.record.subjects:
            subject_list = self.record.subjects.split('|')
            return [f'{subject}||' for subject in subject_list]
        else:
            subject_list.append(f'{self.record.subjects}||')
            return subject_list
    
    def format_rights(self):
        if not self.record.rights: 
            return None

        rights_elements = self.record.rights.split('|')
        rights_status = rights_elements[0]

        if rights_status == 'in copyright':
            return '{}|{}||{}|'.format(self.record.source, 'in_copyright', 'In Copyright') 
        elif rights_status == 'public domain':
            return '{}|{}||{}|'.format(self.record.source, 'public_domain', 'Public Domain') 
        
        return None
    
    def parse_permissions(self, permissions: str) -> dict:
        if permissions == LimitedAccessPermissions.FULL_ACCESS.value:
            return {"is_downloadable": True, "requires_login": False}
        if permissions == LimitedAccessPermissions.PARTIAL_ACCESS.value:
            return {"is_downloadable": False, "requires_login": False}
        if permissions == LimitedAccessPermissions.LIMITED_DOWNLOADABLE.value:
            return {"is_downloadable": True, "requires_login": True}
        else:
            return {"is_downloadable": False, "requires_login": True}
