import json
import os
from time import sleep

from .record_frbrizer import RecordFRBRizer
from .record_clusterer import RecordClusterer
from .record_deleter import RecordDeleter
from .record_file_saver import RecordFileSaver
from .link_fulfiller import LinkFulfiller

from logger import create_log
from managers import DBManager, ElasticsearchManager, RabbitMQManager, S3Manager
from model import Record

logger = create_log(__name__)


class RecordPipelineProcess:

    def __init__(self, *args):
        self.db_manager = DBManager()

        self.record_queue = os.environ['RECORD_PIPELINE_QUEUE']
        self.record_route = os.environ['RECORD_PIPELINE_ROUTING_KEY']

        self.queue_manager = RabbitMQManager()
        self.queue_manager.create_connection()
        self.queue_manager.create_or_connect_queue(self.record_queue, self.record_route)

        self.storage_manager = S3Manager()

        self.es_manager = ElasticsearchManager()
        self.es_manager.create_elastic_connection()

        self.record_file_saver = RecordFileSaver(storage_manager=self.storage_manager)
        self.record_frbrizer = RecordFRBRizer(db_manager=self.db_manager)
        self.record_clusterer = RecordClusterer(db_manager=self.db_manager)
        self.link_fulfiller = LinkFulfiller(db_manager=self.db_manager)
        self.record_deleter = RecordDeleter(db_manager=self.db_manager, store_manager=self.storage_manager, es_manager=self.es_manager)

    def runProcess(self, max_attempts: int=4):
        try:
             for attempt in range(0, max_attempts):
                wait_time = 30 * attempt

                if wait_time:
                    logger.info(f'Waiting {wait_time}s for record messages')
                    sleep(wait_time)

                while message := self.queue_manager.get_message_from_queue(self.record_queue):
                    message_props, _, message_body = message

                    if not message_props or not message_body:
                        break
                    
                    self._process_message(message)
        except Exception:
            logger.exception('Failed to run record pipeline process')
        finally:
            self.queue_manager.close_connection()
            if self.db_manager.engine: 
                self.db_manager.engine.dispose()
    
    def _process_message(self, message):
        try:
            message_props, _, message_body = message
            record = self._parse_message(message_body=message_body)

            self.db_manager.create_session()

            if record._deletion_flag is True:
                self.record_deleter.delete_record(record)
            else:
                self.record_file_saver.save_record_files(record)
                saved_record = self._save_record(record)
                frbrized_record = self.record_frbrizer.frbrize_record(saved_record)
                clustered_records = self.record_clusterer.cluster_record(frbrized_record)
                self.link_fulfiller.fulfill_records_links(clustered_records)
                
            self.queue_manager.acknowledge_message_processed(message_props.delivery_tag)
        except Exception:
            logger.exception(f'Failed to process record: {record}')
            self.queue_manager.reject_message(delivery_tag=message_props.delivery_tag)         
        finally:
            if self.db_manager.session: 
                self.db_manager.session.close()

    def _parse_message(self, message_body) -> Record:
        message = json.loads(message_body)

        return Record(**message)
    
    def _save_record(self, record: Record) -> Record:
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
