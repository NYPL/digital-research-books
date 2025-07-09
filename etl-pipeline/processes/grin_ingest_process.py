from logger import create_log
from .pdf_generation.pdf_generate import generate_pdf
from .grin.download import GRINDownload
from managers import SQSManager
import os
import json

SQS_VISIBILITY_TIMEOUT_SECS = 90 * 60


class GRINIngestProcess:
    def __init__(self, *args):
        self.sqs_manager = SQSManager(queue_name=os.environ["GRIN_INGEST_SQS_QUEUE"], max_receive_count=1)

        self.bucket = os.environ["PRIVATE_FILE_BUCKET"]

    def runProcess(self):
        sqs_messages = self.sqs_manager.get_messages_from_queue(
            SQS_VISIBILITY_TIMEOUT_SECS
        )

        if not sqs_messages:
            return

        barcode, receipt_handle = self._parse_message(sqs_messages[0])

        grin_download = GRINDownload(barcode, self.bucket)
        ocr_dir, mets_file = grin_download.run_process()

        generate_pdf(self.bucket, self.barcode, ocr_dir, mets_file)

        self.sqs_manager.acknowledge_message_processed(receipt_handle)

    def _parse_message(self, sqs_message):
        receipt_handle = sqs_message["ReceiptHandle"]
        message_body = json.loads(sqs_message["Body"])
        barcode = message_body["barcode"]

        return barcode, receipt_handle
