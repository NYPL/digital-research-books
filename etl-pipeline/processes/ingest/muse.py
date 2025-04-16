import os

from managers import DBManager, MUSEManager, RabbitMQManager, S3Manager
from mappings.muse import map_muse_record
from services import MUSEService
from model import get_file_message
from logger import create_log
from ..record_buffer import RecordBuffer
from .. import utils

logger = create_log(__name__)


class MUSEProcess():
    MARC_URL = 'https://about.muse.jhu.edu/lib/metadata?format=marc&content=book&include=oa&filename=open_access_books&no_auth=1'
    MARC_CSV_URL = 'https://about.muse.jhu.edu/static/org/local/holdings/muse_book_metadata.csv'
    MUSE_ROOT_URL = 'https://muse.jhu.edu'

    def __init__(self, *args):
        self.params = utils.parse_process_args(*args)

        self.db_manager = DBManager()
        self.db_manager.create_session()

        self.record_buffer = RecordBuffer(db_manager=self.db_manager)

        self.muse_service = MUSEService()

        self.s3_manager = S3Manager()

        self.file_queue = os.environ['FILE_QUEUE']
        self.file_route = os.environ['FILE_ROUTING_KEY']

        self.rabbitmq_manager = RabbitMQManager()
        self.rabbitmq_manager.create_connection()
        self.rabbitmq_manager.create_or_connect_queue(self.file_queue, self.file_route)

    def runProcess(self):

        records = self.muse_service.get_records(
            start_timestamp=utils.get_start_datetime(process_type=self.params.process_type, ingest_period=self.params.ingest_period),
            limit=self.params.limit,
            offset=self.params.offset,
            record_id=self.params.record_id
        )
        for record in records:
            # Use the available source link to create a PDF manifest file and
            # store in S3
            _, muse_link, _, muse_type, _ = list(
                record.has_part[0].split('|')
            )

            muse_manager = MUSEManager(record, muse_link, muse_type)

            muse_manager.parse_muse_page()

            muse_manager.identify_readable_versions()

            muse_manager.add_readable_links()

            if muse_manager.pdf_webpub_manifest:
                self.s3_manager.put_object(
                    muse_manager.pdf_webpub_manifest.toJson().encode('utf-8'),
                    muse_manager.s3_pdf_read_path,
                    muse_manager.s3_bucket
                )

            if muse_manager.epub_url:
                self.rabbitmq_manager.send_message_to_queue(self.file_queue, self.file_route, get_file_message(muse_manager.epub_url, muse_manager.s3_epub_path))

            self.record_buffer.add(record=record)

        self.record_buffer.flush()

        logger.info(f'Ingested {self.record_buffer.ingest_count} MUSE records')
