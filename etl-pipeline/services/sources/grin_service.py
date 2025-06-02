from datetime import datetime
import os
from typing import Optional, Generator

from managers import S3Manager
from model import Record
from .source_service import SourceService


class GRINService(SourceService):
    def __init__(self):
        self.environment = os.environ["ENVIRONMENT"]
        self.file_locations = [
            f"drb-files-{self.environment}",
            f"drb-files-{self.environment}-limited",
        ]
        self.storage_manager = S3Manager()

    def get_records(
        self,
        start_timestamp: Optional[datetime] = None,
        offset: int = 0,
        limit: int = None,
    ) -> Generator[Record, None, None]:
        for file_location in self.file_locations:
            object_paginator = self.storage_manager.client.get_paginator('list_objects_v2')
            object_iterator = object_paginator.paginate(Bucket=file_location, Prefix='/todo/add/prefix')
            
            if start_timestamp:
                object_iterator = object_iterator.search(f"Contents[?to_string(LastModified) > '\"{start_timestamp}\"'].Key")
            
            for objects in object_iterator:
                for content in objects['Contents']:
                    key = content['Key']
                    
                    # TODO: get mets file, map to record, yield record

        return

    def get_record(self, record_id):
        return
