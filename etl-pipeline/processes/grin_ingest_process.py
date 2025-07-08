from logger import create_log
from .pdf_generation.pdf_generate import PDFGenerationProcess
from .grin.download import GRINDownload
from .record_ingestor import RecordIngestor
from model import Source
from managers import SQSManager
from mappings.marc_record import map_marc_record
import os
import json

SQS_VISIBILITY_TIMEOUT_SECS = 90 * 60

class GRINIngestProcess:
    def __init__(self, *args):
        self.sqs_manager = SQSManager(queue_name="test-queue", max_receive_count=1)

        self.bucket = (
            "drb-files-limited-production"
            if os.environ.get("ENVIRONMENT", "qa") == "production"
            else "drb-files-limited-qa"
        )

    def runProcess(self):
        sqs_messages = self.sqs_manager.get_messages_from_queue(SQS_VISIBILITY_TIMEOUT_SECS)

        if not sqs_messages:
            return
        
        barcode, receipt_handle = self._parse_message(sqs_messages[0])

        grin_download = GRINDownload(barcode, self.bucket)
        ocr_dir, mets_file = grin_download.run_process()

        pdf_generation = PDFGenerationProcess(self.bucket, ocr_dir, mets_file)
        pdf_generation.run_process()

        self.sqs_manager.acknowledge_message_processed(receipt_handle)
        # pdf_url = self.bucket.get_public_url(pdf_key)
        # record_ingestor = RecordIngestor(Source.GRIN.value)
        # record = map_marc_record(mets_file, source=Source.GRIN, pdf_url=pdf_url)
        # record_ingestor.ingest(record)
    
    def _parse_message(self, sqs_message):
        receipt_handle = sqs_message["ReceiptHandle"]
        message_body = json.loads(sqs_message["Body"])
        barcode = message_body['barcode']
        
        return barcode, receipt_handle
