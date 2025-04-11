from logger import create_log
from model import Source
from services import METService
from ..record_ingestor import RecordIngestor
from .. import utils

logger = create_log(__name__)

class METProcess():
    def __init__(self, *args):
        self.met_service = METService()
        self.params = utils.parse_process_args(*args)
        self.record_ingestor = RecordIngestor(source_service=self.met_service, source=Source.MET.value)

    def runProcess(self) -> int:
        return self.record_ingestor.ingest(params=self.params)
