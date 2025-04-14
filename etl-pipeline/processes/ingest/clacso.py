from services import DSpaceService

from logger import create_log
from model import Source
from mappings.clacso import CLACSOMapping
from ..record_ingestor import RecordIngestor
from .. import utils

logger = create_log(__name__)

class CLACSOProcess():
    CLACSO_BASE_URL = 'https://biblioteca-repositorio.clacso.edu.ar/oai/request?'

    def __init__(self, *args):
        self.dspace_service = DSpaceService(base_url=self.CLACSO_BASE_URL, source_mapping=CLACSOMapping)
        self.params = utils.parse_process_args(*args)
        self.record_ingestor = RecordIngestor(source=Source.CLACSO.value)

    def runProcess(self):
        start_timestamp = utils.get_start_datetime(
            process_type=self.params.process_type, ingest_period=self.params.ingest_period,
        ),
        return self.record_ingestor.ingest(
            self.dspace_service.get_records(
                start_timestamp=start_timestamp,
                offset=self.params.offset,
                limit=self.params.limit,
            ),
        )
