from logger import create_log
from model import Source
from services import METService
from ..record_ingestor import RecordIngestor
from .. import utils

logger = create_log(__name__)


class METProcess:
    def __init__(self, *args):
        self.met_service = METService()
        self.params = utils.parse_process_args(*args)
        self.record_ingestor = RecordIngestor(source=Source.MET.value)

    def runProcess(self) -> int:
        start_timestamp = utils.get_start_datetime(
            process_type=self.params.process_type,
            ingest_period=self.params.ingest_period,
        )

        return self.record_ingestor.ingest(
            self.met_service.get_records(
                start_timestamp=start_timestamp,
                offset=self.params.offset,
                limit=self.params.limit,
            ),
        )
