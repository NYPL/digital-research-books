# Script run daily to scrape and initialize conversion for GRIN books acquired in the previous day
# Temporarily, script will also intialize conversion for backfilled books

from sqlalchemy import update
from .grin_client import GRINClient
import pandas as pd
from model import GRINState, GRINStatus, Record, FRBRStatus
from typing import List, Iterator
from managers import DBManager
from uuid import uuid4
from logger import create_log

CHUNK_SIZE = 1000


class GRINConversion:
    def __init__(self):
        self.client = GRINClient()
        self.db_manager = DBManager()
        self.logger = create_log(__name__)

    def run_process(self):
        self.db_manager.create_session()

        self.acquire_and_convert_new_books()

        self.process_converted_books()

        self.db_manager.session.close()

    def acquire_and_convert_new_books(self):
        data = self.client.acquired_today()
        if len(data) > 1:
            new_books_df = self.transform_scraped_data(data)

            new_barcodes = new_books_df.query('State == "NEW"')
            converted_data = self.client.convert(new_barcodes["Barcode"])
            converted_df = self.transform_scraped_data(converted_data)

            converted_barcodes = converted_df.query('Status == "Success"')

            self.save_barcodes(converted_barcodes["Barcode"], GRINState.CONVERTING)

    def convert_backfills(self):
        # initialize conversion the new books, and update the DB
        pass

    def save_barcodes(self, barcodes, state):
        if len(barcodes) > 0:
            for chunked_barcodes in chunk(iter(barcodes), CHUNK_SIZE):
                records: List[Record] = []
                for barcode in chunked_barcodes:
                    records.append(
                        Record(
                            uuid=uuid4(),
                            frbr_status=FRBRStatus.TODO.value,
                            source_id=f"{barcode}|grin",
                            grin_status=GRINStatus(
                                barcode=barcode, failed_download=0, state=state.value
                            ),
                        )
                    )
            try:
                self.db_manager.session.add_all(records)
                self.db_manager.commit_changes()
            except Exception:
                self.logger.exception("Failed to add the following records:")
                raise

    def process_converted_books(self):
        converted_barcodes = self.client.converted_filenames()

        if len(converted_barcodes) > 0:
            for chunked_barcodes in chunk(iter(converted_barcodes), CHUNK_SIZE):
                stripped_barcodes: List[str] = []
                for barcode in chunked_barcodes:
                    barcode = barcode.split(".", 1)[0]  # converted file name has the following pattern 1234.tar.gz.gpg
                    stripped_barcodes.append(barcode)

                try:
                    update_barcodes = (
                        update(GRINStatus)
                        .filter(GRINStatus.barcode.in_(stripped_barcodes))
                        .filter(GRINStatus.state != GRINState.DOWNLOADED.value)
                        .values(state=GRINState.CONVERTED.value)
                    )
                    self.db_manager.session.execute(update_barcodes)
                    self.db_manager.commit_changes()
                except:
                    self.db_manager.session.rollback()
                    raise

    def transform_scraped_data(self, data):
        headers = data[0].split("\t")
        rows = []
        for row in data[1:]:
            if row != "":
                rows.append(row.split("\t"))

        return pd.DataFrame(rows, columns=headers)


def chunk(xs: Iterator, size: int) -> Iterator[list]:
    while True:
        chunk = []
        try:
            for _ in range(size):
                chunk.append(next(xs))
            yield chunk
        except StopIteration:
            if chunk:
                yield chunk

            break


if __name__ == "__main__":
    grin_conversion = GRINConversion()
    grin_conversion.run_process()
