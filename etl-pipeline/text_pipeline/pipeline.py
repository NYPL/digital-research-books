import argparse
import os

from args_parser import env_parser
from digital_assets import get_stored_file_url
from logger import create_log
import file_conversion.pdfs.mets_parser as mets_parser
from managers import DBManager, ElasticsearchManager, S3Manager
from model import Record, Part, FileFlags, Source
from utils import with_logging, setup_env
from processes.record_embedder import RecordEmbedder
from processes.record_pipeline import RecordPipelineProcess

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
        self.record_pipeline = RecordPipelineProcess()

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

            mets_file = mets_parser.METSFile.from_mets_str(
                self.storage_manager.get_object(
                    key=f"grin/{barcode}/NYPL_{barcode}.xml",
                    bucket=os.environ["PRIVATE_FILE_BUCKET"],
                )["Body"].read()
            )
            first_page_part = Part(
                index=1,
                url=get_stored_file_url(
                    storage_name=os.environ["PRIVATE_FILE_BUCKET"],
                    file_path=f"grin/{barcode}/{mets_file.first_page}",
                ),
                source=Source.GRIN.value,
                file_type="application/ocr",
                flags=str(FileFlags(reader=True)),
            )
            record.has_part = [str(first_page_part)]

            db_manager.session.commit()
            db_manager.session.refresh(record)

        self.record_embedder.embed(record, barcode)


if __name__ == "__main__":
    args = parser.parse_args()

    text_pipeline = TextPipeline()
    text_pipeline.run(args.barcode)
