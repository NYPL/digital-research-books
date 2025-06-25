from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generator

from mappings.record_mapping import RecordMapping
from model import Record


class SourceService(ABC):
    @abstractmethod
    def get_records(
        self,
        start_timestamp: datetime | None = None,
        offset: int = 0,
        limit: int | None = None,
    ) -> list[RecordMapping] | Generator[Record, None, None]:
        pass

    @abstractmethod
    def get_record(self, record_id: str) -> Record:
        pass
