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
        self.record_ingestor = RecordIngestor(source_service=self.nypl_bib_service, source=Source.NYPL.value)

    def runProcess(self):    
        return self.record_ingestor.ingest(params=self.params)
