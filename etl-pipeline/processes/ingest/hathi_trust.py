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
        self.record_ingestor = RecordIngestor(source_service=self.hathi_trust_service, source=Source.HATHI.value)

    def runProcess(self) -> int:
        return self.record_ingestor.ingest(params=self.params)
