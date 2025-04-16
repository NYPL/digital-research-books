from logger import create_log
from model import Source
from ..record_ingestor import RecordIngestor
from services import NYPLBibService
from .. import utils

logger = create_log(__name__)


class NYPLProcess():
    def __init__(self, *args):
        self.nypl_bib_service = NYPLBibService()
        self.params = utils.parse_process_args(*args)
        self.record_ingestor = RecordIngestor(source=Source.NYPL.value)

    def runProcess(self):
        start_timestamp = utils.get_start_datetime(
            process_type=self.params.process_type,
            ingest_period=self.params.ingest_period,
        )
        
        return self.record_ingestor.ingest(
            self.nypl_bib_service.get_records(
                start_timestamp=start_timestamp,
                offset=self.params.offset,
                limit=self.params.limit,
            ),
        )
