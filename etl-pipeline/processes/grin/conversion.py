from sqlalchemy import update
from .grin_client import GRINClient
import pandas as pd
from sqlalchemy import select
from model import GRINState, GRINStatus, Record, RecordState, FRBRStatus, Source
from typing import List
import time
from managers import DBManager
from uuid import uuid4
from logger import create_log
from utils.chunker import chunk
from .. import utils


class GRINConversion:
    def __init__(self, *args, batch_limit=1000):
        self.params = utils.parse_process_args(*args)
        self.client = GRINClient()
        self.logger = create_log(__name__)
        self.batch_limit = batch_limit

    def runProcess(self):
        if self.params.process_type == "daily":
            with DBManager() as self.db_manager:
                self.convert_new_barcodes()
                return

        while self._get_unconverted_barcode_count > 0:
            with DBManager() as self.db_manager:
                try:
                    self.convert_barcodes_pending_conversion()
                    self.sync_converted_books()
                except Exception:
                    self.logger.exception("Failed to run GRIN conversion process")

            time.sleep(120)

    def convert_new_barcodes(self):
        new_barcodes = self.client.acquired_today()

        if len(new_barcodes) <= 2:
            self.logger.info("No new barcodes")
            return

        new_barcodes = self._transform_scraped_data(new_barcodes)
        converting_barcodes, _ = self._convert_barcodes(new_barcodes["Barcode"])

        self._save_barcodes(converting_barcodes, GRINState.CONVERTING)

    def convert_barcodes_pending_conversion(self):
        barcodes_pending_conversion = (
            self.db_manager.session.execute(
                (
                    select(GRINStatus.barcode)
                    .where(
                        GRINStatus.state == GRINState.PENDING_CONVERSION.value,
                    )
                    .where(GRINStatus.date_created <= GRINStatus.backfill_timestamp())
                    .limit(self.batch_limit)
                )
            )
            .scalars()
            .all()
        )

        if not barcodes_pending_conversion:
            self.logger.info("No barcodes pending conversion.")
            return

        converting_barcodes, converted_barcodes = self._convert_barcodes(
            barcodes_pending_conversion
        )

        self._update_grin_state(
            converting_barcodes,
            old_state=GRINState.PENDING_CONVERSION,
            new_state=GRINState.CONVERTING,
        )

        self._update_grin_state(
            converted_barcodes,
            old_state=GRINState.PENDING_CONVERSION,
            new_state=GRINState.CONVERTED,
        )

    def sync_converted_books(self):
        converted_filenames = self.client.converted_filenames()

        if not converted_filenames:
            return

        for chunked_filenames in chunk(iter(converted_filenames), self.batch_limit):
            # converted file name has the following pattern 1234.tar.gz.gpg
            converted_barcodes = {
                barcode.split(".", 1)[0] for barcode in chunked_filenames
            }

            try:
                update_results = self.db_manager.session.execute(
                    update(GRINStatus)
                    .filter(GRINStatus.barcode.in_(list(converted_barcodes)))
                    .filter(GRINStatus.state != GRINState.DOWNLOADED.value)
                    .filter(GRINStatus.state != GRINState.CONVERTED.value)
                    .values(state=GRINState.CONVERTED.value)
                )
                self.db_manager.commit_changes()

                if update_results.rowcount:
                    self.logger.info(f"Converted {update_results.rowcount} barcodes")
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

    def _convert_barcodes(self, barcodes):
        converted_data = self.client.convert(barcodes)
        converted_df = self._transform_scraped_data(converted_data)

        converting_barcodes = converted_df.query(
            "Status in ('Success', 'Already being converted')"
        )
        converted_barcodes = converted_df.query(
            "Status=='Already available for download'"
        )
        converting_barcodes_list = converting_barcodes["Barcode"].to_list()
        converted_barcodes_list = converted_barcodes["Barcode"].to_list()
        return converting_barcodes_list, converted_barcodes_list

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
                .filter(GRINStatus.state == GRINState.PENDING_CONVERSION.value)
                .filter(GRINStatus.state == GRINState.CONVERTING.value)
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
