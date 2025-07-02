from logger import create_log
from .pdf_generation.pdf_generate import PDFGenerationProcess
from .grin.download import GRINDownload
from .record_ingestor import RecordIngestor
from model import Source
from mappings.marc_record import map_marc_record
import os


class GRINIngestProcess:
    def __init__(self, *args, sqs_message):
        # TODO: When we start consuming SQS messages, change setup here accordingly.
        self.barcode = sqs_message["barcode"]

        self.bucket = (
            "drb-files-limited-production"
            if os.environ.get("ENVIRONMENT", "qa") == "production"
            else "drb-files-limited-qa"
        )

    def runProcess(self):
        grin_download = GRINDownload(self.barcode, self.bucket)
        ocr_dir, mets_file = grin_download.run_process()

        pdf_generation = PDFGenerationProcess(self.bucket, ocr_dir, mets_file)
        pdf_key = pdf_generation.run_process()

        # pdf_url = f"https://{self.bucket}.s3.amazonaws.com/tagged_pdfs/{pdf_key}"
        # record_ingestor = RecordIngestor(Source.GRIN.value)
        # record = map_marc_record(mets_file, source=Source.GRIN, pdf_url=pdf_url)
        # record_ingestor.ingest(record)
