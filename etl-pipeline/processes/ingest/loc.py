from services import LOCService

from logger import create_log
from model import Source
from ..record_ingestor import RecordIngestor
from .. import utils

logger = create_log(__name__)


class LOCProcess():

    def __init__(self, *args):
        self.loc_service = LOCService()
        self.params = utils.parse_process_args(*args)
        self.record_ingestor = RecordIngestor(source_service=self.loc_service, source=Source.LOC.value)

    def runProcess(self):    
        return self.record_ingestor.ingest(params=self.params)
