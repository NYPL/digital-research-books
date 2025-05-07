from logger import create_log
from processes import HathiTrustProcess, RecordPipelineProcess

logger = create_log(__name__)


class SeedLocalDataProcess:
    def __init__(self, *args):
        self.hath_trust_process = HathiTrustProcess(
            "weekly", None, None, None, 50, None
        )
        self.record_pipeline_process = RecordPipelineProcess()

    def runProcess(self):
        try:
            self.hath_trust_process.runProcess()
            self.record_pipeline_process.runProcess(max_attempts=2)
        except Exception as e:
            logger.exception(f"Failed to seed local data")
            raise e
