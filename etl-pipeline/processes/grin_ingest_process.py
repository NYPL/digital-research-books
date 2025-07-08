from logger import create_log
from .pdf_generation.pdf_generate import generate_pdf_process
from .grin.download import GRINDownload
import os


class GRINIngestProcess:
    def __init__(self, *args):
        # TODO: When we start consuming SQS messages, change setup here accordingly.
        self.barcode = args[3]

        self.bucket = f"drb-files-limited-{os.environ['ENVIRONMENT']}"

    def runProcess(self):
        grin_download = GRINDownload(self.barcode, self.bucket)
        ocr_dir, mets_file = grin_download.run_process()

        pdf_key = generate_pdf_process(self.bucket, self.barcode, ocr_dir, mets_file)
