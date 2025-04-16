from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generator, Optional, Union

from mappings.record_mapping import RecordMapping
from model import Record

class SourceService(ABC):

    @abstractmethod
    def get_records(
        self,
        start_timestamp: Optional[datetime]=None,
        offset: int=0,
        limit: Optional[int]=None,
        record_id: Optional[str]=None
    ) -> Union[list[RecordMapping], Generator[Record, None, None]]:
        pass
