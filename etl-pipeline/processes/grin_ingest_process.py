from logger import create_log
from pdf_generation.pdf_generate import PDFGenerationProcess
from grin.download import GRINDownload
import os

class GRINIngestProcess:
    def __init__(self, sqs_message):
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
        pdf_generation.run_process()