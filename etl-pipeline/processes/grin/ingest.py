import io
import json
import os
import re

from datetime import datetime
from logger import create_log
from .download import GRINDownloadService
from managers import SQSManager, S3Manager
from model import Record, Source, FileFlags, Part
from ..record_ingestor import RecordIngestor
from services.rights_determiner import determine_rights
from pymarc import parse_xml_to_array
from mappings.marc_record import map_marc_record
from time import sleep
from .. import utils
from utils.timer import timer
from file_conversion.pdfs import mets_parser
from digital_assets import get_stored_file_url

SQS_VISIBILITY_TIMEOUT_SECS = 90 * 60
logger = create_log(__name__)


class GRINIngestProcess:
    def __init__(self, *args):
        self.params = utils.parse_process_args(*args)
        self.bucket = os.environ["PRIVATE_FILE_BUCKET"]

        self.sqs_manager = SQSManager(queue_name=os.environ["GRIN_INGEST_SQS_QUEUE"])
        self.storage_manager = S3Manager()
        self.record_ingestor = RecordIngestor(Source.GRIN.value)
        self.grin_download_service = GRINDownloadService(bucket=self.bucket)

        self.processed_count = 0

    def run(self, max_attempts: int = 10):
        try:
            for attempt in range(max_attempts):
                wait_time = 5 * attempt
                if wait_time:
                    logger.info(f"Waiting {wait_time}s for GRIN messages")
                    sleep(wait_time)

                while messages := self.sqs_manager.get_messages_from_queue(
                    visibility_timeout=SQS_VISIBILITY_TIMEOUT_SECS
                ):
                    for message in messages:
                        self._process_message(message)

                        if (
                            self.params.limit is not None
                            and self.processed_count >= self.params.limit
                        ):
                            logger.info(
                                f"Reached GRIN ingest limit: {self.params.limit}"
                            )
                            return
        except Exception:
            logger.exception("Failed to run GRIN ingest process")

    @timer(logger)
    def _process_message(self, message):
        try:
            barcodes, receipt_handle = self._parse_message(message)

            for barcode in barcodes:
                self._process_barcode(barcode)
                self.processed_count += 1

            self.sqs_manager.acknowledge_message_processed(receipt_handle)
        except Exception:
            logger.exception("Failed to process GRIN ingest message")
            self.sqs_manager.reject_message(receipt_handle)

    def _process_barcode(self, barcode):
        try:
            _, mets_file = self.grin_download_service.download_barcode(barcode)

            record = self._map_record(barcode, mets_file)
            if self._is_in_public_domain(record):
                skip_next = False
            else:
                skip_next = (
                    self.params.options.get("skip_next", "false").lower() == "true"
                )
            self.record_ingestor.ingest(
                [record],
                skip_next=skip_next,
            )
        except Exception:
            logger.exception(f"Failed to process barcode: {barcode}")

    def _parse_message(self, sqs_message):
        receipt_handle = sqs_message["ReceiptHandle"]
        message_body = json.loads(sqs_message["Body"])
        barcodes = message_body["barcodes"]

        return barcodes, receipt_handle

    def _map_record(self, barcode, mets_file: str) -> Record:
        xml_metadata = self.storage_manager.get_object(
            key=mets_file, bucket=self.bucket
        )["Body"].read()
        xml_file = io.BytesIO(xml_metadata)

        marc_records = parse_xml_to_array(xml_file)
        marc_record = marc_records[-1]

        record = map_marc_record(marc_record, source=Source.GRIN)
        record.source_id = f"{barcode}|grin"

        try:
            rights = determine_rights(barcode)

            if rights:
                record.rights = rights
        except Exception:
            logger.exception(f"Failed to determine rights for barcode: {barcode}")

        if self._is_in_public_domain(record) and record.has_part is None:
            mets_file = mets_parser.METSFile.from_mets_str(xml_metadata)
            first_page_part = self._create_first_page_part(barcode, mets_file)
            record.has_part = [str(first_page_part)]

        return record

    def _is_in_public_domain(self, record) -> bool:
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

    def _create_first_page_part(
        self, barcode: str, mets_file: mets_parser.METSFile
    ) -> Part:
        if not mets_file:
            return None

        return Part(
            index=1,
            url=get_stored_file_url(
                storage_name=os.environ["PRIVATE_FILE_BUCKET"],
                file_path=f"grin/{barcode}/{mets_file.first_page}",
            ),
            source=Source.GRIN.value,
            file_type="application/ocr",
            flags=str(FileFlags(reader=True)),
        )
