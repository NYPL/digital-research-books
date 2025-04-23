import csv
from datetime import datetime
from itertools import islice
from io import BytesIO
from pymarc import MARCReader
import requests

from managers import S3Manager, MUSEManager
from mappings.muse import map_muse_record
from model import Record
from logger import create_log
from typing import Generator, Optional
from .source_service import SourceService

logger = create_log(__name__)

MARC_URL = 'https://about.muse.jhu.edu/lib/metadata?format=marc&content=book&include=oa&filename=open_access_books&no_auth=1&bookid=123'
MARC_CSV_URL = 'https://about.muse.jhu.edu/static/org/local/holdings/muse_book_metadata.csv'
MUSE_ROOT_URL = 'https://muse.jhu.edu'


class MUSEService(SourceService):

    def __init__(self):
        self.store_manager = S3Manager()

    def get_records(
        self,
        start_timestamp: Optional[datetime]=None,
        offset: int=0,
        limit: Optional[int]=None,
    ) -> Generator[Record, None, None]:
        record_updates = self._get_record_updates()
        record_count = 0

        for marc_record in MARCReader(self._get_marc_records()):
            if limit and record_count >= limit:
                break

            if start_timestamp and self._get_record_updated_at(marc_record, record_updates) >= start_timestamp:
                continue

            try:
                yield self._map_marc_record(marc_record)
                record_count += 1
            except Exception:
                logger.exception('Unable to parse MUSE record')

    def get_record(self, record_id: str) -> Record:
        for marc_record in MARCReader(self._get_marc_records()):
            if self._get_record_id(marc_record) == record_id:
                return self._map_marc_record(marc_record)
            
        raise Exception(f'MUSE record not found with id: {record_id}')
    
    def _map_marc_record(self, marc_record) -> Record:
        record = map_muse_record(marc_record)
        first_part = record.parts[0]

        muse_manager = MUSEManager(record, first_part.url, first_part.file_type)

        muse_manager.parse_muse_page()
        muse_manager.identify_readable_versions()
        muse_manager.add_readable_links()

        if muse_manager.pdf_webpub_manifest:
            self.store_manager.put_object(
                muse_manager.pdf_webpub_manifest.toJson().encode('utf-8'),
                muse_manager.s3_pdf_read_path,
                muse_manager.s3_bucket
            )
        
        return record

    def _get_marc_records(self):
        try:
            marc_response = requests.get(MARC_URL, stream=True, timeout=30)
            marc_response.raise_for_status()
        except Exception:
            raise Exception('Unable to load Project MUSE MARC file')

        content = bytes()
        for chunk in marc_response.iter_content(1024 * 250):
            content += chunk

        return BytesIO(content)

    def _get_record_updates(self) -> dict:
        try:
            muse_metadata_response = requests.get(MARC_CSV_URL, stream=True, timeout=30)
            muse_metadata_response.raise_for_status()
        except Exception as e:
            raise Exception('Unable to load Project MUSE metadata')

        record_updates = {}

        for record_update in islice(csv.reader(muse_metadata_response.iter_lines(decode_unicode=True), skipinitialspace=True), 4, None):
            try:
                record_id = record_update[7]
                updated_at = record_update[11]
                record_updates[record_id] = datetime.strptime(updated_at, '%Y-%m-%d')
            except (IndexError, ValueError):
                logger.exception(f'Unable to get record update: {record_update}')

        return record_updates

    def _get_record_id(self, record) -> str:
        record_url = record.get_fields('856')[0]['u']

        return record_url[:-1].split('/')[-1]
    
    def _get_record_updated_at(self, record, record_updates: dict) -> datetime:        
        return record_updates.get(self._get_record_id(record), datetime(1970, 1, 1))
