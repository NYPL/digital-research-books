from mappings.gutenberg import GutenbergMapping
from model import Source
from ..record_ingestor import RecordIngestor
from services import GutenbergService
from .. import utils


class GutenbergProcess:
    def __init__(self, *args):
        self.params = utils.parse_process_args(*args)

        self.gutenberg_service = GutenbergService()
        self.record_ingestor = RecordIngestor(source=Source.GUTENBERG.value)

    def runProcess(self) -> int:
        records = self.gutenberg_service.get_records(
            start_timestamp=utils.get_start_datetime(
                process_type=self.params.process_type,
                ingest_period=self.params.ingest_period,
            ),
            offset=self.params.offset,
            limit=self.params.limit,
        )
        return self.record_ingestor.ingest(records)
