from datetime import datetime
import json
import requests
import urllib.parse
from typing import Generator, Optional

from logger import create_log
from mappings.publisher_backlist import PublisherBacklistMapping
from model import Record
from services.ssm_service import SSMService
from .source_service import SourceService

logger = create_log(__name__)

BASE_URL = "https://api.airtable.com/v0/appBoLf4lMofecGPU/Publisher%20Backlists%20%26%20Collections%20%F0%9F%93%96?view=All%20Lists"
PAGE_SIZE = 100


class PublisherBacklistService(SourceService):
    def __init__(self):
        self.ssm_service = SSMService()
        self.airtable_auth_token = self.ssm_service.get_parameter("airtable/pub-backlist/api-key")     

    def get_records(
        self,
        start_timestamp: datetime = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Generator[Record, None, None]:
        filter_by_formula = self._build_filter_by_formula_parameter(start_timestamp)
        url = f"{BASE_URL}&pageSize={PAGE_SIZE}{filter_by_formula}"
        headers = { "Authorization": f"Bearer {self.airtable_auth_token}" }
        record_count = 0

        while response := requests.get(url, headers=headers):
            response_data = response.json()

            for record in response_data.get("records", []):
                try:
                    record_metadata = record.get('fields')
                    
                    publisher_record = PublisherBacklistMapping(record_metadata)
                    publisher_record.applyMapping()
                    
                    yield publisher_record.record

                    record_count += 1

                    if limit and record_count >= limit:
                        return
                except Exception:
                    logger.exception(f"Failed to process Publisher Backlist record: {record_metadata}")

            if "offset" not in response_data:
                break

            url = f"{BASE_URL}&pageSize={PAGE_SIZE}{filter_by_formula}&offset={response_data['offset']}"

    def _build_filter_by_formula_parameter(self, start_timestamp: Optional[datetime]=None) -> str:
        ready_to_ingest_filter = urllib.parse.quote("{DRB_Ready to ingest} = TRUE()")

        if not start_timestamp:
            return f"&filterByFormula={ready_to_ingest_filter}"

        start_date_time_str = start_timestamp.strftime("%Y-%m-%d")
        is_same_date_time_filter = urllib.parse.quote(f'IS_SAME({{Last Modified}}, "{start_date_time_str}")')
        is_after_date_time_filter = urllib.parse.quote(f'IS_AFTER({{Last Modified}}, "{start_date_time_str}")')

        return f"&filterByFormula=AND(OR({is_same_date_time_filter}),{is_after_date_time_filter})),{ready_to_ingest_filter})"
