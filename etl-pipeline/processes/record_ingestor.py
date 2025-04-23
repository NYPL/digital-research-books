import json
import os
import typing

from logger import create_log
from managers import RabbitMQManager, SQSManager
from model import Record

logger = create_log(__name__)


class RecordIngestor:

    def __init__(self, source: str):
        self.source = source

        self.records_queue = os.environ['RECORD_PIPELINE_QUEUE']
        self.records_route = os.environ['RECORD_PIPELINE_ROUTING_KEY']

        self.queue_manager = RabbitMQManager()
        self.queue_manager.create_connection()
        self.queue_manager.create_or_connect_queue(self.records_queue, self.records_route)

        sqs_records_queue = os.environ["RECORD_PIPELINE_SQS_QUEUE"]
        self.sqs_manager = SQSManager(queue_name=sqs_records_queue)


    def ingest(self, records: typing.Iterator[Record]) -> int:
        ingest_count = 0

        try:
            for record in records:
                message = json.dumps(record.to_dict(), default=str)
                self.queue_manager.send_message_to_queue(
                    queue_name=self.records_queue,
                    routing_key=self.records_route,
                    message=message,
                )
                ingest_count += 1
                try:
                    self.sqs_manager.send_message_to_queue(message)
                except Exception as e:
                    logger.exception(f"Failed to send message to SQS")

        except Exception:
            logger.exception(f'Failed to ingest {self.source} records')
        finally:
            self.queue_manager.close_connection()

        logger.info(f'Ingested {ingest_count} {self.source} records')
        return ingest_count
