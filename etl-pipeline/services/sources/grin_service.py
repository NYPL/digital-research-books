from datetime import datetime
import os
from typing import Optional

from managers import S3Manager
from .source_service import SourceService


class GRINService(SourceService):
    def __init__(self):
        self.environment = os.environ["ENVIRONMENT"]
        self.file_locations = [
            f"drb-files-{self.environment}",
            f"drb-files-{self.enviroment}-limited",
        ]
        self.storage_manager = S3Manager()

    def get_records(
        self,
        start_timestamp: Optional[datetime] = None,
        offset: int = 0,
        limit: int = None,
    ):
        return

    def get_record(self, record_id):
        return
