from logger import create_log
from model import Source
from services import HathiTrustService
from ..record_ingestor import RecordIngestor
from .. import utils

logger = create_log(__name__)

class HathiTrustProcess():
    def __init__(self, *args):
        self.hathi_trust_service = HathiTrustService()
        self.params = utils.parse_process_args(*args)
        self.record_ingestor = RecordIngestor(source=Source.HATHI.value)

    def runProcess(self) -> int:
        start_timestamp = utils.get_start_datetime(
            process_type=self.params.process_type, ingest_period=self.params.ingest_period,
        ),
        return self.record_ingestor.ingest(
            self.hathi_trust_service.get_records(
                start_timestamp=start_timestamp,
                offset=self.params.offset,
                limit=self.params.limit,
            ),
        )
