from datetime import datetime
from logger import create_log
from download import GRINDownload
from managers import SQSManager, S3Manager
from logger import create_log
import os
import json
from xml.etree import ElementTree as ET
from model import Record, Source, FileFlags, Part
from ..record_ingestor import RecordIngestor
from pymarc import parse_xml_to_array
import io
import re
from mappings.marc_record import map_marc_record

SQS_VISIBILITY_TIMEOUT_SECS = 90 * 60
logger = create_log(__name__)


class GRINIngestProcess:
    def __init__(self, *args):
        self.sqs_manager = SQSManager(
            queue_name=os.environ["GRIN_INGEST_SQS_QUEUE"], max_receive_count=1
        )

        self.bucket = os.environ["PRIVATE_FILE_BUCKET"]
        self.storage_manager = S3Manager()
        self.record_ingestor = RecordIngestor(Source.GRIN.value)

    def runProcess(self):
        # TODO: continuously get messages from the SQS queue

        try:
            sqs_messages = self.sqs_manager.get_messages_from_queue(
                SQS_VISIBILITY_TIMEOUT_SECS
            )
        except Exception:
            logger.exception("Failed to run GRIN Ingest Process")
            return

        if not sqs_messages:
            return

        barcode, receipt_handle = self._parse_message(sqs_messages[0])

        grin_download = GRINDownload(barcode, self.bucket)
        ocr_dir, mets_file = grin_download.run_process()

        # Convert METs file to record
        # Ingest record

        self.sqs_manager.acknowledge_message_processed(receipt_handle)

    def _parse_message(self, sqs_message):
        receipt_handle = sqs_message["ReceiptHandle"]
        message_body = json.loads(sqs_message["Body"])
        barcode = message_body["barcode"]

        return barcode, receipt_handle

    def _map_record_and_rights(self, xml_data: ET.Element) -> tuple:
        xml_bytes = ET.tostring(self, xml_data, encoding="utf-8")
        xml_file = io.BytesIO(xml_bytes)
        marc_records = parse_xml_to_array(xml_file)
        marc_record = marc_records[-1]
        record = map_marc_record(marc_record, source=Source.GRIN)
        is_public_domain = self._is_in_public_domain(record)
        return record, is_public_domain

    def _ingest_record(
        record: Record, barcode: str, pdf_url: str, is_public_domain: bool
    ):
        record_ingestor = RecordIngestor(Source.GRIN.value)

        record.source_id = f"{barcode}|grin"
        record.has_part.append(
            str(
                Part(
                    index=1,
                    source=Source.GRIN.value,
                    url=pdf_url,
                    file_type="application/pdf",
                    flags=str(FileFlags(download=True))
                    if is_public_domain
                    else str(FileFlags()),
                )
            )
        )

        record_ingestor.ingest([record])

    def _is_in_public_domain(record) -> bool:
        rights = record.rights.lower() if record.rights else ""
        is_public_domain = "public_domain" in rights or "public domain" in rights

        if not record.dates:
            return is_public_domain

        year = int(re.search(r"[0-9]{4}", record.dates[0]).group(0))
        publication_date = datetime.date(year, 1, 1)
        current_year = datetime.date.today().year

        threshold_year = current_year - 95

        public_domain_threshold_date = datetime.date(threshold_year, 1, 1)

        return publication_date < public_domain_threshold_date or is_public_domain
