import os
from services import DSpaceService
from logger import create_log
from mappings.doab import DOABMapping
from managers import DBManager, DOABLinkManager, S3Manager, RabbitMQManager
from model import get_file_message, Source
from ..record_buffer import RecordBuffer, Record
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
        self.dspace_service = DSpaceService(base_url=self.DOAB_BASE_URL, source_mapping=DOABMapping)
        self.params = utils.parse_process_args(*args)
        self.record_ingestor = RecordIngestor(source_service=self.dspace_service, source=Source.DOAB.value)

    def runProcess(self) -> int:
        return self.record_ingestor.ingest(params=self.params, source_identifier=self.DOAB_IDENTIFIER)
