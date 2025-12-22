from datetime import datetime, timedelta, timezone
import os
from .grin_client import GRINClient
import pandas as pd
from sqlalchemy import select, or_, update
from model import GRINState, GRINStatus, Record, RecordState, FRBRStatus, Source
from typing import List
import time
from managers import DBManager, SQSManager
from uuid import uuid4
from logger import create_log
from utils.chunker import chunk
from .. import utils

IN_PROCESS_LIMIT = 50000
DELAY_IN_SECONDS = 60
CONVERSION_LIMIT = 5000


class GRINConversion:
    """
    Check the GRIN API for the state of books in the digitization process and
    update their states in the GRIN STATUSES DB table. If the books have finished
    digitization (i.e. converted) and are ready for ingest, send to the GRIN
    ingest SQS queue.

    If process_type='daily', check the status of barcodes which started
    conversion/digitization in the last 24 hrs and barcodes which are currently
    listed by GRIN as failed.

    Otherwise, keep polling the GRIN API for current status until no rows in
    the grin_statuses table are "PENDING_CONVERSION" or "CONVERTING" status
    (i.e. all have succeeded or failed).
    """

    def __init__(self, *args, batch_limit=1000):
        self.params = utils.parse_process_args(*args)
        self.client = GRINClient()
        self.sqs_manager = SQSManager(queue_name=os.environ["GRIN_INGEST_SQS_QUEUE"])
        self.logger = create_log(__name__)
        self.batch_limit = batch_limit

    def run(self):
        if self.params.process_type == "daily":
            with DBManager() as self.db_manager:
                self.convert_new_barcodes()
                self.convert_failed_barcodes()
                self._sync_and_send_converted_barcodes()
                return

        while (barcodes_remaining := self._get_unconverted_barcode_count()) > 0:
            self.logger.info(f"{barcodes_remaining} barcodes need to be converted")

            with DBManager() as self.db_manager:
                try:
                    self.convert_barcodes_pending_conversion()
                    self._sync_and_send_converted_barcodes()
                except Exception:
                    self.logger.exception("Failed to run GRIN conversion process")

            self.logger.info(f"Polling in {DELAY_IN_SECONDS} seconds")
            time.sleep(DELAY_IN_SECONDS)

    def convert_new_barcodes(self):
        new_barcodes = self.client.get_new_barcodes()

        if len(new_barcodes) == 0:
            self.logger.info("No new barcodes")
            return

        converting_barcodes, *_ = self._convert_barcodes(new_barcodes)

        self._save_barcodes(converting_barcodes, GRINState.CONVERTING)

    def convert_barcodes_pending_conversion(self):
        in_process_data = self.client.in_process()
        in_process_count = len(in_process_data)

        available_to_process_count = IN_PROCESS_LIMIT - in_process_count

        if available_to_process_count == 0:
            self.logger.info(f"Hit in process limit of {IN_PROCESS_LIMIT} barcodes.")
            return

        conversion_limit = min(available_to_process_count, CONVERSION_LIMIT)

        self.logger.info(f"Converting {conversion_limit} pending conversion")

        barcodes_pending_conversion = (
            self.db_manager.session.execute(
                (
                    select(GRINStatus.barcode)
                    .where(
                        GRINStatus.state == GRINState.PENDING_CONVERSION.value,
                    )
                    .where(GRINStatus.date_created <= GRINStatus.backfill_timestamp())
                    .limit(conversion_limit)
                )
            )
            .scalars()
            .all()
        )

        if not barcodes_pending_conversion:
            self.logger.info("No barcodes pending conversion.")
            return

        converting_barcodes, converted_barcodes, unavailable_barcodes = (
            self._convert_barcodes(barcodes_pending_conversion)
        )

        self._update_grin_state(
            converting_barcodes,
            old_state=GRINState.PENDING_CONVERSION,
            new_state=GRINState.CONVERTING,
        )

        self._update_grin_state(
            converted_barcodes,
            old_state=GRINState.CONVERTING,
            new_state=GRINState.CONVERTED,
        )

        self._update_grin_state(
            unavailable_barcodes,
            old_state=GRINState.PENDING_CONVERSION,
            new_state=GRINState.UNAVAILABLE,
        )

    def convert_failed_barcodes(self):
        failed_conversion_barcodes = self.client.failed_conversion()

        if failed_conversion_barcodes:
            failed_barcodes = (
                self.db_manager.session.execute(
                    (
                        select(GRINStatus.barcode).where(
                            GRINStatus.state == GRINState.CONVERTING.value,
                            GRINStatus.barcode.in_(failed_conversion_barcodes),
                            GRINStatus.date_modified
                            <= datetime.now(timezone.utc) - timedelta(weeks=2),
                        )
                    )
                )
                .scalars()
                .all()
            )

            self.logger.info(f"Converting {len(failed_barcodes)} failed conversions")

            if len(failed_barcodes) > 0:
                converting_barcodes, *_ = self._convert_barcodes(failed_barcodes)
                self._update_grin_date_modified(converting_barcodes)
            else:
                self.logger.info("No failed conversion barcodes from last two weeks")

    def _sync_converted_books(self) -> set:
        converted_filenames = self.client.converted_filenames()
        newly_converted_barcodes = set()

        if not converted_filenames:
            return

        for chunked_filenames in chunk(iter(converted_filenames), self.batch_limit):
            # converted file name has the following pattern 1234.tar.gz.gpg
            converted_barcodes = {
                barcode.split(".", 1)[0] for barcode in chunked_filenames
            }

            try:
                barcodes_to_update = (
                    self.db_manager.session.execute(
                        select(GRINStatus.barcode)
                        .filter(GRINStatus.barcode.in_(list(converted_barcodes)))
                        .filter(
                            or_(
                                GRINStatus.state == GRINState.PENDING_CONVERSION.value,
                                GRINStatus.state == GRINState.CONVERTING.value,
                            )
                        )
                    )
                    .scalars()
                    .all()
                )

                if barcodes_to_update:
                    update_results = self.db_manager.session.execute(
                        update(GRINStatus)
                        .filter(GRINStatus.barcode.in_(barcodes_to_update))
                        .values(state=GRINState.CONVERTED.value)
                    )

                    self.db_manager.commit_changes()

                    self.logger.info(f"Converted {update_results.rowcount} barcodes")
                    newly_converted_barcodes.update(barcodes_to_update)
            except Exception:
                self.db_manager.session.rollback()
                self.logger.exception(
                    f"Failed to update the following converted records: {converted_filenames}"
                )

            existing_barcodes = {
                grin_status.barcode
                for grin_status in self.db_manager.session.query(GRINStatus.barcode)
                .filter(GRINStatus.barcode.in_(converted_barcodes))
                .all()
            }

            missing_from_table = converted_barcodes - existing_barcodes
            self._save_barcodes(missing_from_table, GRINState.CONVERTED)
            newly_converted_barcodes.update(missing_from_table)

        return newly_converted_barcodes

    def _send_converted_barcodes_for_download(self, converted_barcodes):
        for chunked_barcodes in chunk(iter(converted_barcodes), 10):
            self.sqs_manager.send_message_to_queue({"barcodes": chunked_barcodes})

        self.logger.info(
            f"Sent {len(converted_barcodes)} converted barcodes for download"
        )

    def _sync_and_send_converted_barcodes(self):
        converted_barcodes = self._sync_converted_books()
        if converted_barcodes:
            self._send_converted_barcodes_for_download(converted_barcodes)

    def _convert_barcodes(self, barcodes):
        converted_data = self.client.convert(barcodes)

        if not converted_data:
            return [], [], []

        converted_df = self._transform_scraped_data(converted_data)

        converting_barcodes_list = []
        converted_barcodes_list = []
        unavailable_barcodes_list = []

        for _, row in converted_df.iterrows():
            status = row["Status"]
            barcode = row["Barcode"]

            if status in ("Success", "Already being converted"):
                converting_barcodes_list.append(barcode)
            elif status == "Already available for download":
                converted_barcodes_list.append(barcode)
            elif status == "Not allowed to be downloaded":
                unavailable_barcodes_list.append(barcode)

        return (
            converting_barcodes_list,
            converted_barcodes_list,
            unavailable_barcodes_list,
        )

    def _save_barcodes(self, barcodes, state: GRINState):
        if not barcodes:
            return

        records: List[Record] = []

        for barcode in barcodes:
            records.append(
                Record(
                    uuid=uuid4(),
                    frbr_status=FRBRStatus.TODO.value,
                    cluster_status=False,
                    source_id=f"{barcode}|grin",
                    state=RecordState.STAGED.value,
                    source=Source.GRIN.value,
                    grin_status=GRINStatus(
                        barcode=barcode, failed_download=0, state=state.value
                    ),
                )
            )

        try:
            self.db_manager.session.add_all(records)
            self.db_manager.commit_changes()
            self.logger.info(f"Saved {len(records)} barcodes in state: {state.value}")
        except Exception:
            self.db_manager.session.rollback()
            self.logger.exception(
                f"Failed to save {len(barcodes)} barcodes in state: {state.value}"
            )

    def _transform_scraped_data(self, data):
        headers = data[0].split("\t")
        rows = []

        for row in data[1:]:
            if row != "":
                rows.append(row.split("\t"))

        return pd.DataFrame(rows, columns=headers)

    def _get_unconverted_barcode_count(self) -> int:
        with DBManager() as db_manager:
            return (
                db_manager.session.query(GRINStatus)
                .filter(
                    or_(
                        GRINStatus.state == GRINState.PENDING_CONVERSION.value,
                        GRINStatus.state == GRINState.CONVERTING.value,
                    )
                )
                .count()
            )

    def _update_grin_state(self, barcodes, old_state: GRINState, new_state: GRINState):
        try:
            state_results = self.db_manager.session.execute(
                update(GRINStatus)
                .filter(GRINStatus.barcode.in_(barcodes))
                .values(state=new_state.value)
            )
            self.db_manager.commit_changes()

            self.logger.info(
                f"Updated {state_results.rowcount} barcodes state from {old_state.value} to {new_state.value}"
            )
        except Exception:
            self.db_manager.session.rollback()
            self.logger.exception(
                f"Failed to update {len(barcodes)} barcodes from {old_state.value} to {new_state.value}"
            )

    def _update_grin_date_modified(self, barcodes):
        try:
            update_results = self.db_manager.session.execute(
                update(GRINStatus)
                .filter(GRINStatus.barcode.in_(barcodes))
                .values(date_modified=datetime.now(timezone.utc))
            )
            self.db_manager.commit_changes()

            self.logger.info(
                f"Updated {update_results.rowcount} barcodes date modified"
            )
        except Exception:
            self.db_manager.session.rollback()
            self.logger.exception(
                f"Failed to update {len(barcodes)} barcodes date modified"
            )
