# Script run daily to scrape and initialize conversion for GRIN books acquired in the previous day
# Temporarily, script will also intialize conversion for backfilled books

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

        self.acquire_new_books()

        self.process_converted_books()

        self.db_manager.session.close()

    def acquire_new_books(self):
        data = self.client.acquired_today()
        if len(data) > 1:
            dataframe = self.transform_scraped_data(data)

            grin_converted_barcodes = dataframe.query('State == "CONVERTED"')
            self.save_barcodes(grin_converted_barcodes["Barcode"], GRINState.CONVERTED)

            new_barcodes = dataframe.query('State == "NEW"')
            self.save_barcodes(new_barcodes["Barcode"], GRINState.PENDING_CONVERSION)

    def convert(self):
        # Add conversion step
        pass

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
        converted_barcodes = self.client.converted()

        if len(converted_barcodes) > 0:
            for barcode in converted_barcodes:
                try:
                    self.db_manager.session.query(GRINStatus).filter(
                        GRINStatus.barcode == f"{barcode}",
                        GRINStatus.state != GRINState.DOWNLOADED.value,
                        GRINStatus.state != GRINState.CONVERTED.value,
                    ).update({"state": GRINState.CONVERTED.value})
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
