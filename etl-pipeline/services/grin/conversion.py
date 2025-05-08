# Script run daily to scrape and initialize conversion for GRIN books acquired in the previous day
# Temporarily, script will also intialize conversion for backfilled books
from .grin_client import GRINClient
import pandas as pd
import os
from model import GRINState, GRINStatus, Record, FRBRStatus
from typing import List, Iterator
from managers import DBManager
from uuid import uuid4

CHUNK_SIZE = 1000

class GRINConversion:
    def __init__(self):
        self.client = GRINClient()
        self.db_manager = DBManager(
            user=os.environ.get("POSTGRES_USER", None),
            pswd=os.environ.get("POSTGRES_PSWD", None),
            host=os.environ.get("POSTGRES_HOST", None),
            port=os.environ.get("POSTGRES_PORT", None),
            db=os.environ.get("POSTGRES_NAME", None),
        )
        self.db_manager.create_session()
        
        self.acquire_new_books()

        self.db_manager.session.close()
        pass

    def acquire_new_books(self):
        data = self.client.acquired_today()
        if len(data) > 1:
            dataframe = self.transform_scraped_data(data)

            barcodes_needing_conversion = dataframe.query('State == "NEW"')
            self.save_barcodes(barcodes_needing_conversion, "PENDING_CONVERSION")

            newly_converted_barcodes = dataframe.query('State == "CONVERTED"')
            self.save_barcodes(newly_converted_barcodes, "CONVERTED")
            
    def convert(self):
        # convert new books within the month
        pass

    def convert_backfills(self):
        # initialize conversion the new books, and update the DB
        pass

    def get_converted(self):
        # scrape the converted files from GRIN and update the DB
        pass

    def transform_scraped_data(self, data):
        headers = data[0].split('\t')
        rows = []
        for row in data[1:]:
            if row != '':
                rows.append(row.split('\t'))
        
        return pd.DataFrame(rows, columns=headers)

    def save_barcodes(self, barcodes, status):
        for chunked_barcodes in chunk(iter(barcodes), CHUNK_SIZE):
            records: List[Record] = []
            
            for barcode in chunked_barcodes:
                records.append(
                    Record(
                        uuid=uuid4(),
                        frbr_status=FRBRStatus.TODO.value,
                        source_id=f"{barcode}|grin",
                        grin_status=GRINStatus(
                            barcode=barcode,
                            failed_download=0,
                            state=GRINState.status.value,
                        ),
                    )
                )
        try:
            self.db_manager.session.bulk_save_objects(records)
            self.db_manager.commit_changes()
        except Exception:
            self.logging.exception("Failed to add the following records")
            raise


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

conversion = GRINConversion()