import json
import os
import typing

from logger import create_log
from managers import DBManager, RabbitMQManager, SQSManager
from model import Record

logger = create_log(__name__)


class RecordIngestor:

    def __init__(self, source: str):
        self.source = source

        self.db_manager = DBManager()
        self.db_manager.create_session()

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
                # TODO: save and send messages in bulk
                saved_record = self._save_record(record)
                
                message = { 'record_id': saved_record.id }

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
    
    def _save_record(self, record: Record) -> Record:
        # TODO: set record state to ingested
        existing_record = (
            self.db_manager.session.query(Record)
                .filter(Record.source_id == record.source_id)
                .filter(Record.source == record.source)
                .first()
        )

        if existing_record:
            record = self._update_record(record, existing_record)
        else:
            self.db_manager.session.add(record)

        self.db_manager.session.commit()
        self.db_manager.session.refresh(record)

        logger.info(f'{"Updated" if existing_record else "Created"} record: {record}')
        return record
    
    def _update_record(self, record: Record, existing_record: Record) -> Record:
        for attribute, value in record:
            if attribute == 'uuid': 
                continue

            setattr(existing_record, attribute, value)

        return existing_record
