from datetime import datetime
import os
from pymarc import parse_xml_to_array
from typing import Optional, Generator

from managers import S3Manager
from mappings.marc_record import map_marc_record
from model import Record
from .source_service import SourceService


class GRINService(SourceService):
    def __init__(self):
        self.environment = os.environ["ENVIRONMENT"]
        self.metadata_file_storage = f"drb-files-{self.environment}-limited"
        self.storage_manager = S3Manager()

    def get_records(
        self,
        start_timestamp: Optional[datetime] = None,
        offset: int = 0,
        limit: int = None,
    ) -> Generator[Record, None, None]:
        object_paginator = self.storage_manager.client.get_paginator("list_objects_v2")
        object_iterator = object_paginator.paginate(
            Bucket=self.metadata_file_storage, Prefix="/todo/add/prefix"
        )
        record_count = 0

        if start_timestamp:
            object_iterator = object_iterator.search(
                f"Contents[?to_string(LastModified) > '\"{start_timestamp}\"'].Key"
            )

        for objects in object_iterator:
            for content in objects["Contents"]:
                key = content["Key"]

                if key.endswith(".xml"):
                    metadata_file = self.storage_manager.get_object(
                        key, bucket=self.metadata_file_storage
                    )
                    marc_record = parse_xml_to_array(metadata_file)

                    yield map_marc_record(marc_record[0])
                    record_count += 1

                if record_count >= limit:
                    return
