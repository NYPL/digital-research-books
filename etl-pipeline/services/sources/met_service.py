from datetime import datetime
import requests
from typing import Generator, Optional, Union

from logger import create_log
from mappings.met import map_met_record
from model import Record
from .source_service import SourceService

logger = create_log(__name__)


class METService(SourceService):
    # The documentation for these API queries is here: https://help.oclc.org/Metadata_Services/CONTENTdm/Advanced_website_customization/API_Reference/CONTENTdm_API/CONTENTdm_Server_API_Functions_-_dmwebservices
    LIST_QUERY = 'https://libmma.contentdm.oclc.org/digital/bl/dmwebservices/index.php?q=dmQuery/p15324coll10/CISOSEARCHALL/title!dmmodified!dmcreated!rights/dmmodified/{}/{}/1/0/0/00/0/json'
    ITEM_INFO_QUERY = 'https://libmma.contentdm.oclc.org/digital/bl/dmwebservices/index.php?q=dmGetItemInfo/p15324coll10/{}/json'
    COMPOUND_QUERY = 'https://libmma.contentdm.oclc.org/digital/bl/dmwebservices/index.php?q=dmGetCompoundObjectInfo/p15324coll10/{}/json'
    IMAGE_QUERY = 'https://libmma.contentdm.oclc.org/digital/api/singleitem/collection/p15324coll10/id/{}'

    def __init__(self):
        pass

    def get_records(
        self,
        start_timestamp: Optional[datetime]=None,
        offset: int=0,
        limit: Optional[int]=None
    ) -> Generator[Record, None, None]:
        current_position = offset
        page_size = 50

        while met_records := self._get_met_records(page_size=page_size, current_position=current_position):
            for record in met_records:
                modified_at = record.get('dmmodified')
                rights = record.get('rights')

                if (start_timestamp and datetime.strptime(modified_at, '%Y-%m-%d') >=  start_timestamp) or 'copyright' in rights.lower():
                    continue

                mapped_met_record = self._map_met_record(record)

                if mapped_met_record is not None:
                    yield mapped_met_record

            if limit and current_position >= limit:
                break

            current_position += page_size

    def query_met_api(self, query: str, method: str='GET') -> Union[str, dict]:
        method = method.upper()

        response = requests.request(method, query, timeout=30)

        response.raise_for_status()

        if method == 'HEAD':
            return response.status_code
        else:
            return response.json()

    def _get_met_records(self, page_size: int=50, current_position: int=0) -> list:
        try:
            records_response = self.query_met_api(query=self.LIST_QUERY.format(page_size, current_position))

            return records_response.get('records', [])     
        except Exception:
            logger.exception('Faile to get met records')   
            return []

    def _map_met_record(self, record: dict) -> Optional[Record]:
        try:            
            met_record = self.query_met_api(query=self.ITEM_INFO_QUERY.format(record.get('pointer')))

            return map_met_record(met_record)
        except Exception:
            logger.exception('Failed to process MET record')
            return None
        
    def get_record(self, record_id: str):
        pass
