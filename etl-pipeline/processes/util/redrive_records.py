import json
import os

from logger import create_log
from managers import DBManager, SQSManager
from model import Record
from .. import utils

logger = create_log(__name__)


class RedriveRecordsProcess:

    def __init__(self, *args):
        self.params = utils.parse_process_args(*args)

        self.db_manager = DBManager()
        self.db_manager.create_session()

        record_pipeline_queue = os.environ["RECORD_PIPELINE_SQS_QUEUE"]
        self.sqs_manager = SQSManager(record_pipeline_queue)

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
                self.sqs_manager.send_message_to_queue(
                    message={ "source_id": record.source_id, "source": record.source }
                )

                redrive_count = count

                if self.params.limit and redrive_count >= self.params.limit:
                    break


            logger.info(f'Redrove {redrive_count} {self.params.source} records')
        except Exception as e:
            logger.info(f'Failed to redrive {self.params.source} records for source')
        finally:
            self.db_manager.close_connection()
