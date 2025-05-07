from services import MUSEService
from model import Source
from logger import create_log
from ..record_ingestor import RecordIngestor
from .. import utils

logger = create_log(__name__)


class MUSEProcess:
    def __init__(self, *args):
        self.params = utils.parse_process_args(*args)

        self.muse_service = MUSEService()
        self.record_ingestor = RecordIngestor(source=Source.MUSE.value)

    def runProcess(self) -> int:
        if self.params.record_id:
            return self.record_ingestor.ingest(
                [self.muse_service.get_record(record_id=self.params.record_id)]
            )

        return self.record_ingestor.ingest(
            self.muse_service.get_records(
                start_timestamp=utils.get_start_datetime(
                    process_type=self.params.process_type,
                    ingest_period=self.params.ingest_period,
                ),
                limit=self.params.limit,
                offset=self.params.offset,
            )
        )
