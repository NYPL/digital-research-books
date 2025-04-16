from datetime import datetime
import requests
import urllib.parse
from typing import Generator, Optional
from model import Record

from logger import create_log
from mappings.publisher_backlist import PublisherBacklistMapping
from services.ssm_service import SSMService
from .source_service import SourceService

logger = create_log(__name__)

BASE_URL = "https://api.airtable.com/v0/appBoLf4lMofecGPU/Publisher%20Backlists%20%26%20Collections%20%F0%9F%93%96?view=All%20Lists"


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
        records = self.get_publisher_backlist_records(start_timestamp, offset, limit)

        for record in records:
            try:
                record_metadata = record.get('fields')

                publisher_backlist_record = PublisherBacklistMapping(record_metadata)
                publisher_backlist_record.applyMapping()
                
                yield publisher_backlist_record.record
            except Exception:
                logger.exception(f"Failed to process Publisher Backlist record: {record_metadata}")

    def build_filter_by_formula_parameter(self, start_timestamp: Optional[datetime]=None) -> str:
        ready_to_ingest_filter = urllib.parse.quote("{DRB_Ready to ingest} = TRUE()")

        if not start_timestamp:
            return f"&filterByFormula={ready_to_ingest_filter}"

        start_date_time_str = start_timestamp.strftime("%Y-%m-%d")
        is_same_date_time_filter = urllib.parse.quote(f'IS_SAME({{Last Modified}}, "{start_date_time_str}")')
        is_after_date_time_filter = urllib.parse.quote(f'IS_AFTER({{Last Modified}}, "{start_date_time_str}")')

        return f"&filterByFormula=AND(OR({is_same_date_time_filter}),{is_after_date_time_filter})),{ready_to_ingest_filter})"

    def get_publisher_backlist_records(
        self,
        start_timestamp: datetime = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> list[dict]:
        filter_by_formula = self.build_filter_by_formula_parameter(start_timestamp)
        url = f"{BASE_URL}&pageSize={limit}{filter_by_formula}"
        headers = {"Authorization": f"Bearer {self.airtable_auth_token}"}
        publisher_backlist_records = []

        records_response = requests.get(url, headers=headers)
        records_response_json = records_response.json()

        publisher_backlist_records.extend(records_response_json.get("records", []))

        while "offset" in records_response_json:
            next_page_url = url + f"&offset={records_response_json['offset']}"

            records_response = requests.get(next_page_url, headers=headers)
            records_response_json = records_response.json()

            publisher_backlist_records.extend(records_response_json.get("records", []))
        
        return publisher_backlist_records
