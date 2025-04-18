from services import PublisherBacklistService

from logger import create_log
from ..record_ingestor import RecordIngestor
from .. import utils

logger = create_log(__name__)


class PublisherBacklistProcess():
    def __init__(self, *args):
        self.params = utils.parse_process_args(*args)

        self.record_ingestor = RecordIngestor(source='Publisher Backlist')
        self.publisher_backlist_service = PublisherBacklistService()
        
    def runProcess(self) -> int:
        start_timestamp = utils.get_start_datetime(
            process_type=self.params.process_type,
            ingest_period=self.params.ingest_period,
        )

        return self.record_ingestor.ingest(
            self.publisher_backlist_service.get_records(
                start_timestamp=start_timestamp,
                offset=self.params.offset,
                limit=self.params.limit
            )
        )
