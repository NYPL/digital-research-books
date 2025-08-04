from logger import create_log
from model import Source
from processes import IngestProcess, RecordPipelineProcess

logger = create_log(__name__)


class SeedLocalDataProcess:
    def __init__(self, *args):
        self.hath_trust_process = IngestProcess(
            "weekly", None, None, None, 50, None, Source.HATHI.value
        )
        self.record_pipeline_process = RecordPipelineProcess()

    def run(self):
        try:
            self.hath_trust_process.run()
            self.record_pipeline_process.run(max_attempts=2)
        except Exception as e:
            logger.exception("Failed to seed local data")
            raise e
