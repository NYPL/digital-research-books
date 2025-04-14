from model import Source
from services import ChicagoISACService
from ..record_ingestor import RecordIngestor
from .. import utils


class ChicagoISACProcess():

    def __init__(self, *args):
        self.chicago_isac_service = ChicagoISACService()
        self.params = utils.parse_process_args(*args)
        self.record_ingestor = RecordIngestor(source=Source.CHICACO_ISAC.value)

    def runProcess(self):
        start_timestamp = utils.get_start_datetime(
            process_type=params.process_type, ingest_period=params.ingest_period,
        ),
        return self.record_ingestor.ingest(
            self.chicago_isac_process.get_records(
                start_timestamp=start_timestamp,
                offset=params.offset,
                limit=params.limit,
            ),
        )
