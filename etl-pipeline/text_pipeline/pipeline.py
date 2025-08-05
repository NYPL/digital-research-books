import argparse

from args_parser import env_parser
from logger import create_log
from managers import DBManager, ElasticsearchManager, S3Manager
from model import Record
from utils import with_logging, setup_env
from processes.record_embedder import RecordEmbedder

parser = argparse.ArgumentParser(
    prog="textpipeline.pipeline",
    description="Runs the text pipeline on a given barcode number",
    parents=[env_parser()],
)
parser.add_argument("-b", "--barcode", required=True, help="The barcode number")


@setup_env(parser)
class TextPipeline:
    def __init__(self):
        self.logger = create_log(__name__)
        self.storage_manager = S3Manager()

        self.es_manager = ElasticsearchManager()
        self.es_manager.create_elastic_connection()

        self.record_embedder = RecordEmbedder(self.es_manager, self.storage_manager)

    @with_logging(__name__)
    def run(self, barcode: str):
        self.logger.info(f"Running text pipeline on {barcode}")

        with DBManager() as db_manager:
            record = (
                db_manager.session.query(Record)
                .filter(Record.source_id == f"{barcode}|grin")
                .first()
            )

        if not record:
            self.logger.warning(f"Barcode {barcode} not found")
            return

        self.record_embedder.embed(record, barcode)


if __name__ == "__main__":
    args = parser.parse_args()

    text_pipeline = TextPipeline()
    text_pipeline.run(args.barcode)
