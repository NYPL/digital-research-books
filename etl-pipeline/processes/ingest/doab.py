from services import DSpaceService
from logger import create_log
from mappings.doab import DOABMapping
from model import get_file_message, Source
from .. import utils

from logger import create_log
from model import Source
from services import DSpaceService
from ..record_ingestor import RecordIngestor
from .. import utils

logger = create_log(__name__)


class DOABProcess():
    DOAB_BASE_URL = 'https://directory.doabooks.org/oai/request?'
    DOAB_IDENTIFIER = 'oai:directory.doabooks.org'

    def __init__(self, *args):
        self.dspace_service = DSpaceService(base_url=self.DOAB_BASE_URL, source_mapping=DOABMapping, source_identifier=self.DOAB_IDENTIFIER)
        self.params = utils.parse_process_args(*args)
        self.record_ingestor = RecordIngestor(source=Source.DOAB.value)

    def runProcess(self) -> int:
        if self.params.record_id is not None:
            return self.record_ingestor.ingest([self.dspace_service.get_record(record_id=self.params.record_id)])

        start_timestamp = utils.get_start_datetime(
            process_type=self.params.process_type,
            ingest_period=self.params.ingest_period,
        )
        
        return self.record_ingestor.ingest(
            self.dspace_service.get_records(
                start_timestamp=start_timestamp,
                offset=self.params.offset,
                limit=self.params.limit
            )
        )
