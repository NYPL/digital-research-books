import json
import os
from typing import Iterator

from logger import create_log
from managers import DBManager, SQSManager
from model import Record, RecordState
from processes.record_buffer import RecordBuffer
from services import monitor

logger = create_log(__name__)


class RecordIngestor:
    def __init__(self, source: str):
        self.source = source

        db_manager = DBManager()
        db_manager.create_session()

        self.record_buffer = RecordBuffer(db_manager=db_manager)

        sqs_records_queue = os.environ["RECORD_PIPELINE_SQS_QUEUE"]
        self.sqs_manager = SQSManager(queue_name=sqs_records_queue)

    def ingest(self, records: Iterator[Record]) -> int:
        try:
            for record in self._persisted_records(records):
                message = {"source_id": record.source_id, "source": record.source}
                self.sqs_manager.send_message_to_queue(message)

        except Exception:
            logger.exception(f"Failed to ingest {self.source} records")

        logger.info(f"Ingested {self.record_buffer.ingest_count} {self.source} records")
        monitor.track_records_ingested(number_of_records=self.record_buffer.ingest_count, source=self.source)
        
        return self.record_buffer.ingest_count

    def _persisted_records(self, records: Iterator[Record]) -> Iterator[Record]:
        for record in records:
            record.state = RecordState.INGESTED.value

            flushed_records = self.record_buffer.add(record)

            if flushed_records:
                yield from flushed_records

        yield from self.record_buffer.flush()
