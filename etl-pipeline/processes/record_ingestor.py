import json
import os

from logger import create_log
from managers import RabbitMQManager
from services.sources.source_service import SourceService
from . import utils

logger = create_log(__name__)


class RecordIngestor:

    def __init__(self, source_service: SourceService, source: str):
        self.source = source
        self.source_service = source_service

        self.records_queue = os.environ['RECORD_PIPELINE_QUEUE']
        self.records_route = os.environ['RECORD_PIPELINE_ROUTING_KEY']
        
        self.queue_mananger = RabbitMQManager()
        self.queue_mananger.create_connection()
        self.queue_mananger.create_or_connect_queue(self.records_queue, self.records_route)

    def ingest(self, params: utils.ProcessParams) -> int:
        ingest_count = 0

        try:
            records = self.source_service.get_records(
                start_timestamp=utils.get_start_datetime(process_type=params.process_type, ingest_period=params.ingest_period),
                offset=params.offset,
                limit=params.limit
            )

            for record in records:
                self.queue_mananger.send_message_to_queue(
                    queue_name=self.records_queue,
                    routing_key=self.records_route,
                    message=json.dumps(record.to_dict(), default=str)
                )

                ingest_count += 1
        except Exception:
            logger.exception(f'Failed to ingest {self.source} records')
        finally:
            self.queue_mananger.close_connection()

        logger.info(f'Ingested {ingest_count} {self.source} records')
        return ingest_count
