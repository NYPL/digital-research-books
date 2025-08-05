from botocore.exceptions import ClientError
import argparse
import os

from args_parser import env_parser
from file_conversion.pdfs.from_ocr import generate_pdf
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

        self._generate_pdf(record, barcode)
        self.record_embedder.embed(record, barcode)

    def _generate_pdf(self, record: Record, barcode: str):
        is_in_copyright = "in_copyright" in record.rights
        pdf_bucket = (
            os.environ["PRIVATE_FILE_BUCKET"]
            if is_in_copyright
            else os.environ["FILE_BUCKET"]
        )
        pdf_permissions = {} if is_in_copyright else {"ACL": "public-read"}

        if self._pdf_exists(barcode, pdf_bucket):
            return

        generate_pdf(
            storage_manager=self.storage_manager,
            bucket_name=os.environ["PRIVATE_FILE_BUCKET"],
            upload_bucket_name=pdf_bucket,
            file_permissions=pdf_permissions,
            barcode=barcode,
            ocr_dir=f"grin/{barcode}",
            mets_file_key=f"grin/{barcode}/NYPL_{barcode}.xml",
        )

    def _pdf_exists(self, barcode: str, pdf_bucket: str) -> bool:
        try:
            self.storage_manager.client.head_object(
                Key=f"pdfs/{barcode}.pdf", Bucket=pdf_bucket
            )

            self.logger.info(f"{barcode}.pdf already generated")
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code != "404" and error_code != "NoSuchKey":
                raise

        return False


if __name__ == "__main__":
    args = parser.parse_args()

    text_pipeline = TextPipeline()
    text_pipeline.run(args.barcode)
