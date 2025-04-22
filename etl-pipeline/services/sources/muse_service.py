import csv
import os
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pymarc import MARCReader
import requests
from requests.exceptions import ReadTimeout, HTTPError

from managers import DBManager, MUSEError, MUSEManager, RabbitMQManager, S3Manager
from mappings.muse import map_muse_record
from model import get_file_message, Record
from logger import create_log
from processes.record_buffer import RecordBuffer
from processes import utils
from typing import Generator, Optional, Union
from .source_service import SourceService

logger = create_log(__name__)

MARC_URL = 'https://about.muse.jhu.edu/lib/metadata?format=marc&content=book&include=oa&filename=open_access_books&no_auth=1'
MARC_CSV_URL = 'https://about.muse.jhu.edu/static/org/local/holdings/muse_book_metadata.csv'
MUSE_ROOT_URL = 'https://muse.jhu.edu'

class MUSEService(SourceService):

    def __init__(self):
        pass

    def get_records(
        self,
        start_timestamp: Optional[datetime]=None,
        offset: int=0,
        limit: Optional[int]=None,
        record_id: Optional[str]=None
    ) -> Generator[Record, None, None]:
        yield from self.import_marc_records(start_timestamp=start_timestamp, limit=limit, record_id=record_id)
        
    def import_marc_records(self, start_timestamp, limit, record_id):
        self.download_record_updates()

        muse_file = self.download_marc_records()

        marc_reader = MARCReader(muse_file)

        processed_record_count = 0

        for marc_record in marc_reader:
            if limit and processed_record_count >= limit:
                break

            if (start_timestamp or record_id) \
                    and self.skip_record_update(marc_record, start_timestamp, record_id)\
                    is True:
                continue

            try:
                mapped_muse_record = map_muse_record(marc_record)
                if mapped_muse_record is not None:
                    yield mapped_muse_record
                    processed_record_count += 1
            except Exception as e:
                logger.warning('Unable to parse MUSE record')

    def download_marc_records(self):
        try:
            muse_response = requests.get(MARC_URL, stream=True, timeout=30)
            muse_response.raise_for_status()
        except Exception:
            raise Exception('Unable to load Project MUSE MARC file')

        content = bytes()
        for chunk in muse_response.iter_content(1024 * 250):
            content += chunk

        return BytesIO(content)

    def download_record_updates(self):
        try:
            csv_response = requests.get(MARC_CSV_URL, stream=True, timeout=30)
            csv_response.raise_for_status()
        except Exception as e:
            raise Exception('Unable to load Project MUSE CSV file')

        csv_reader = csv.reader(
            csv_response.iter_lines(decode_unicode=True),
            skipinitialspace=True,
        )

        for _ in range(4):
            next(csv_reader, None)  # Skip 4 header rows

        self.update_dates = {}
        for row in csv_reader:
            try:
                self.update_dates[row[7]] = \
                    datetime.strptime(row[11], '%Y-%m-%d')
            except (IndexError, ValueError):
                logger.warning('Unable to parse MUSE')
                logger.debug(row)

    def skip_record_update(self, record, start_date, record_id):
        record_url = record.get_fields('856')[0]['u']

        update_date = self.update_dates.get(record_url[:-1], datetime(1970, 1, 1))

        update_url = f'{MUSE_ROOT_URL}/book/{record_id}'

        return (update_date >= start_date) or update_url == record_url[:-1]
