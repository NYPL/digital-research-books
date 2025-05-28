# Script run daily to scrape and initialize conversion for GRIN books acquired in the previous day
# as well as to initialize conversion for a portion of backfilled books

from sqlalchemy import update
from .grin_client import GRINClient
import pandas as pd
from sqlalchemy import select, update
from model import GRINState, GRINStatus, Record, FRBRStatus
from typing import List, Iterator
from managers import DBManager
from uuid import uuid4
from logger import create_log
from .util import chunk
import argparse


class GRINConversion:
    def __init__(self, *args, batch_limit=1000):
        self.client = GRINClient()
        self.logger = create_log(__name__)
        self.batch_limit = batch_limit

    def runProcess(self, backfill=True):
        with DBManager() as self.db_manager:
            self.acquire_and_convert_new_books()

            self.process_converted_books()

            if backfill:
                self.convert_backfills()

    def acquire_and_convert_new_books(self):
        data = self.client.acquired_today()
        if len(data) > 2:
            new_books_df = self.transform_scraped_data(data)
            new_barcodes = new_books_df.query('State == "NEW"')

            converting_barcodes = self.convert_barcodes(new_barcodes["Barcode"])
            
            self.logger.info(f"Acquired and converted {len(converting_barcodes)} books")
            
            self.save_barcodes(converting_barcodes, GRINState.CONVERTING)

    def convert_backfills(self):
        backfill_query = (
            select(GRINStatus.barcode)
            .where(
                GRINStatus.state == GRINState.PENDING_CONVERSION.value,
            )
            .where(GRINStatus.date_created <= GRINStatus.backfill_timestamp())
            .limit(self.batch_limit)
        )

        backfilled_barcodes = (
            self.db_manager.session.execute(backfill_query).scalars().all()
        )
        if len(backfilled_barcodes) > 0:
            converting_barcodes = self.convert_barcodes(backfilled_barcodes)
            try:
                update_barcodes = (
                    update(GRINStatus)
                    .filter(GRINStatus.barcode.in_(converting_barcodes))
                    .values(state=GRINState.CONVERTING.value)
                )
                updated_results = self.db_manager.session.execute(update_barcodes)
                self.db_manager.commit_changes()

                self.logger.info(f"Converted + updated {updated_results.rowcount} backfill books")
            except:
                self.db_manager.session.rollback()
                self.logger.exception(
                    f"Failed to update the following backfilled records: {converting_barcodes}"
                )

    def convert_barcodes(self, barcodes):
        converted_data = self.client.convert(barcodes)
        converted_df = self.transform_scraped_data(converted_data)
        converted_barcodes = converted_df.query('Status == "Success"')
        return converted_barcodes["Barcode"]

    def save_barcodes(self, barcodes, state):
        if barcodes.empty:
            return

        for chunked_barcodes in chunk(iter(barcodes), self.batch_limit):
            records: List[Record] = []
            for barcode in chunked_barcodes:
                records.append(
                    Record(
                        uuid=uuid4(),
                        frbr_status=FRBRStatus.TODO.value,
                        source_id=f"{barcode}|grin",
                        source="grin",
                        grin_status=GRINStatus(
                            barcode=barcode, failed_download=0, state=state.value
                        ),
                    )
                )
        try:
            self.db_manager.session.add_all(records)
            self.db_manager.commit_changes()
        except Exception:
            self.db_manager.session.rollback()
            self.logger.exception(
                f"Failed to update the following records: {chunked_barcodes}"
            )

    def process_converted_books(self):
        converted_barcodes = self.client.converted_filenames()
        # TODO: Remove this line once we make this a recurring task
        converted_barcodes = converted_barcodes[:self.batch_limit]

        if not converted_barcodes:
            return

        for chunked_barcodes in chunk(iter(converted_barcodes), self.batch_limit):
            stripped_barcodes: List[str] = []
            for barcode in chunked_barcodes:
                # converted file name has the following pattern 1234.tar.gz.gpg
                barcode = barcode.split(".", 1)[0]
                stripped_barcodes.append(barcode)

            try:
                update_barcodes = (
                    update(GRINStatus)
                    .filter(GRINStatus.barcode.in_(stripped_barcodes))
                    .filter(GRINStatus.state != GRINState.DOWNLOADED.value)
                    .values(state=GRINState.CONVERTED.value)
                )
                updated_results = self.db_manager.session.execute(update_barcodes)
                self.db_manager.commit_changes()

                self.logger.info(f"Updated {updated_results.rowcount} converted books in DB")
            except:
                self.db_manager.session.rollback()
                self.logger.exception(
                f"Failed to update the following converted records: {chunked_barcodes}"
                )

    def transform_scraped_data(self, data):
        headers = data[0].split("\t")
        rows = []
        for row in data[1:]:
            if row != "":
                rows.append(row.split("\t"))

        return pd.DataFrame(rows, columns=headers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_limit")
    args = parser.parse_args()
    batch_limit = int(args.batch_limit)

    grin_conversion = GRINConversion(batch_limit)
    grin_conversion.runProcess()
