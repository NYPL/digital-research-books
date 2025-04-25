import json
import os

from logger import create_log
from managers import DBManager, RabbitMQManager
from model import Record
from .. import utils

logger = create_log(__name__)


class RedriveRecordsProcess:

    def __init__(self, *args):
        self.params = utils.parse_process_args(*args)

        self.db_manager = DBManager()
        self.db_manager.create_session()

        self.records_queue = os.environ.get('RECORD_PIPELINE_QUEUE')
        self.records_route = os.environ.get('RECORD_PIPELINE_ROUTING_KEY')

        self.rabbitmq_manager = RabbitMQManager()
        self.rabbitmq_manager.create_connection()
        self.rabbitmq_manager.create_or_connect_queue(queue_name=self.records_queue, routing_key=self.records_route)

    def runProcess(self):
        try:
            query_filters = [Record.source == self.params.source]

            if self.params.process_type != 'complete':
                query_filters.append(Record.cluster_status == False)
            
            records = (
                self.db_manager.session.query(Record)
                    .filter(*query_filters)
                    .yield_per(1000)
            )

            redrive_count = 0

            for count, record in enumerate(records, start=1):
                self.rabbitmq_manager.send_message_to_queue(
                    queue_name=self.records_queue, 
                    routing_key=self.records_route, 
                    message={ 'record_id', record.id }
                )

                redrive_count = count

                if self.params.limit and redrive_count >= self.params.limit:
                    break


            logger.info(f'Redrove {redrive_count} {self.params.source} records')
        except Exception as e:
            logger.info(f'Failed to redrive {self.params.source} records for source')
        finally:
            self.db_manager.close_connection()
            self.rabbitmq_manager.close_connection()
