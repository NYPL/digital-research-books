from logger import create_log
from .pdf_generation.pdf_generate import generate_pdf
from .grin.download import GRINDownload
import os


class GRINIngestProcess:
    def __init__(self, *args, sqs_message):
        # TODO: When we start consuming SQS messages, change setup here accordingly.
        self.barcode = sqs_message["barcode"]

        self.bucket = os.environ["PRIVATE_FILE_BUCKET"]

    def runProcess(self):
        grin_download = GRINDownload(self.barcode, self.bucket)
        ocr_dir, mets_file = grin_download.run_process()

        pdf_key = generate_pdf(self.bucket, self.barcode, ocr_dir, mets_file)
