import json
import os
from time import sleep
from typing import Optional, Tuple
from time import perf_counter, sleep
from .record_embellisher import RecordEmbellisher
from .record_clusterer import RecordClusterer
from .record_deleter import RecordDeleter
from .record_file_saver import RecordFileSaver
from .link_fulfiller import LinkFulfiller

from logger import create_log
from managers import DBManager, ElasticsearchManager, S3Manager, SQSManager, RedisManager
from services import monitor
from model import Record

logger = create_log(__name__)

# Keep messages invisible to other consumers for 90 minutes
SQS_VISIBILITY_TIMEOUT_SECS = 90 * 60


class RecordPipelineProcess:
    def __init__(self, *args):
        self.db_manager = DBManager()
        self.sqs_queue_name = os.environ["RECORD_PIPELINE_SQS_QUEUE"]
        self.sqs_manager = SQSManager(queue_name=self.sqs_queue_name)
        self.storage_manager = S3Manager()
        self.es_manager = ElasticsearchManager()
        self.es_manager.create_elastic_connection()
        self.redis_manager = RedisManager()
        self.redis_manager.create_client()

        # Initialize processors
        self.record_file_saver = RecordFileSaver(
            db_manager=self.db_manager,
            storage_manager=self.storage_manager
        )
        self.record_embellisher = RecordEmbellisher(
            db_manager=self.db_manager,
            redis_manager=self.redis_manager
        )
        self.record_clusterer = RecordClusterer(
            db_manager=self.db_manager,
            redis_manager=self.redis_manager
        )
        self.link_fulfiller = LinkFulfiller(db_manager=self.db_manager)
        self.record_deleter = RecordDeleter(
            db_manager=self.db_manager,
            store_manager=self.storage_manager,
            es_manager=self.es_manager
        )

    def process_record(self, record_uuid: str) -> bool:
        """Process a single record through the pipeline"""
        try:
            self.db_manager.create_session()
            record = self.db_manager.session.query(Record).filter(
                Record.uuid == record_uuid
            ).first()

            if not record:
                logger.error(f"Record {record_uuid} not found")
                return False

            # Process the record through each stage
            record_with_files = self.record_file_saver.save_record_files(record)
            embellished_record = self.record_embellisher.embellish_record(record_with_files)
            clustered_records = self.record_clusterer.cluster_record(embellished_record)
            self.link_fulfiller.fulfill_records_links(clustered_records)

            return True

        except Exception as e:
            logger.error(f"Failed to process record {record_uuid}: {str(e)}")
            return False
        finally:
            if self.db_manager.session:
                self.db_manager.session.close()

    def run_process(self, max_attempts: int = 10):
        """Main processing loop that consumes messages from SQS"""
        try:
            for attempt in range(max_attempts):
                wait_time = 5 * attempt
                if wait_time:
                    logger.info(f"Waiting {wait_time}s for record messages")
                    sleep(wait_time)

                while messages := self.sqs_manager.get_messages_from_queue(
                    visibility_timeout=SQS_VISIBILITY_TIMEOUT_SECS
                ):
                    for message in messages:
                        self._process_message(message)
        except Exception:
            logger.exception('Failed to run record pipeline process')
        finally:
            if self.db_manager.engine:
                self.db_manager.engine.dispose()

    def _process_message(self, message):
        """Process a single SQS message"""
        logger.info("Processing message %s", message)
        start = perf_counter()
        try:
            message_body = message["Body"]
            receipt_handle = message["ReceiptHandle"]
            source_id, source = self._parse_message(message_body=message_body)

            self.db_manager.create_session()
            record = (
                self.db_manager.session.query(Record)
                .filter(Record.source_id == source_id)
                .filter(Record.source == source)
                .first()
            )

            if record is None:
                raise Exception(f'{source} record with source_id {source_id} not found')

            # Process through pipeline stages
            record_with_files = self.record_file_saver.save_record_files(record)
            embellished_record = self.record_embellisher.embellish_record(record_with_files)
            clustered_records = self.record_clusterer.cluster_record(embellished_record)
            self.link_fulfiller.fulfill_records_links(clustered_records)

            self.sqs_manager.acknowledge_message_processed(receipt_handle)
        except Exception:
            logger.exception(f'Failed to process message: {message_body}')
            elapsed_time = perf_counter() - start
            monitor.track_record_pipeline_message_failed(elapsed_time, message_body)
            self.sqs_manager.reject_message(receipt_handle)
        else:
            elapsed_time = perf_counter() - start
            monitor.track_record_pipeline_message_succeeded(record, elapsed_time, message_body)
        finally:
            if self.db_manager.session:
                self.db_manager.session.close()

    def _parse_message(self, message_body: str) -> Tuple[str, str]:
        """Parse SQS message body into source_id and source"""
        message_data = json.loads(message_body)
        return message_data['source_id'], message_data['source']