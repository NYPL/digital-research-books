import json
import os
from typing import Iterator

from logger import create_log
from managers import DBManager, RabbitMQManager, SQSManager
from model import Record, RecordState
from processes.record_buffer import RecordBuffer

logger = create_log(__name__)


class RecordIngestor:

    def __init__(self, source: str):
        self.source = source

        db_manager = DBManager()
        db_manager.create_session()

        self.record_buffer = RecordBuffer(db_manager=db_manager)

        self.records_queue = os.environ['RECORD_PIPELINE_QUEUE']
        self.records_route = os.environ['RECORD_PIPELINE_ROUTING_KEY']

        self.queue_manager = RabbitMQManager()
        self.queue_manager.create_connection()
        self.queue_manager.create_or_connect_queue(self.records_queue, self.records_route)

        sqs_records_queue = os.environ["RECORD_PIPELINE_SQS_QUEUE"]
        self.sqs_manager = SQSManager(queue_name=sqs_records_queue)

    def ingest(self, records: Iterator[Record]) -> int:
        try:
            for record in self._persisted_records(records):
                message = { "record_id": record.id }

                self.queue_manager.send_message_to_queue(
                    queue_name=self.records_queue,
                    routing_key=self.records_route,
                    message=message
                )

                try:
                    self.sqs_manager.send_message_to_queue(message)
                except Exception:
                    logger.exception(f"Failed to send message to SQS")
        except Exception:
            logger.exception(f'Failed to ingest {self.source} records')
        finally:
            self.queue_manager.close_connection()

        logger.info(f'Ingested {self.record_buffer.ingest_count} {self.source} records')
        return self.record_buffer.ingest_count

    def _persisted_records(self, records: Iterator[Record]) -> Iterator[Record]:
        for record in records:
            record.state = RecordState.INGESTED.value
            
            flushed_records = self.record_buffer.add(record)
            
            if flushed_records:
                yield from flushed_records

        yield from self.record_buffer.flush()
