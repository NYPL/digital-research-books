from datetime import datetime
import json
import time
from typing import Generator, Optional
import requests

from mappings.loc import map_loc_record
from model import Record
from .source_service import SourceService
from logger import create_log

logger = create_log(__name__)

LOC_ROOT_OPEN_ACCESS = 'https://www.loc.gov/collections/open-access-books/?fo=json&fa=access-restricted%3Afalse&c=50&at=results&sb=timestamp_desc'
LOC_ROOT_DIGIT = 'https://www.loc.gov/collections/selected-digitized-books/?fo=json&fa=access-restricted%3Afalse&c=50&at=results&sb=timestamp_desc' 


class LOCService(SourceService):

    def get_records(
        self,
        start_timestamp: Optional[datetime]=None,
        offset: int=0,
        limit: Optional[int]=None
    ) -> Generator[Record, None, None]:
        yield from self._import_records(loc_collection_url=LOC_ROOT_OPEN_ACCESS, start_timestamp=start_timestamp, limit=limit)
        yield from self._import_records(loc_collection_url=LOC_ROOT_DIGIT, start_timestamp=start_timestamp, limit=limit)                

    def _import_records(
        self, 
        loc_collection_url: str, 
        start_timestamp: Optional[datetime]=None,
        limit: Optional[int]=None
    ) -> Generator[Record, None, None]:
        record_count = 0
        page_number = 0
        completed_import = False

        try:
            while (page_json_data := self._fetch_page_json_data(page_url=f'{loc_collection_url}&sp={page_number}')) is not None:
                for record_data in page_json_data.get('results', []):
                    if start_timestamp:
                        record_timestamp = datetime.strptime(record_data['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')

                        if record_timestamp < start_timestamp:
                            completed_import = True
                            break

                    loc_record = map_loc_record(source_record=record_data)

                    if loc_record is not None:
                        yield loc_record
                        record_count += 1

                    if limit and record_count >= limit:
                        completed_import = True
                        break

                if completed_import:
                    break

                page_number += 1
                time.sleep(5)
        except Exception:
            logger.exception(f'Failed to import LOC records from: {loc_collection_url}')
            return

    def _fetch_page_json_data(self, page_url: str) -> Optional[dict]:
        max_attempts = 3

        for attempt in (0, max_attempts):
            try:
                page_response = requests.get(page_url, headers={ 'Accept': 'application/json' })

                # If we exceed the last page, a bad request will be returned
                if page_response.status_code == 400:
                    return None
                
                page_response.raise_for_status()

                page_data = page_response.content

                return json.loads(page_data.decode('utf-8'))
            except Exception:
                if attempt < max_attempts - 1:
                    continue

                logger.exception(f'Failed to load page data from: {page_url}')
                return {}
            
    def get_record(self, record_id: str):
        pass
