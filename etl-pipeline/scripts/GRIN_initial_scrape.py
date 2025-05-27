from datetime import datetime
from logger import create_log
from typing import List, Iterator
from model import GRINState, GRINStatus, Record, FRBRStatus
from managers import DBManager
from uuid import uuid4
from services.grin.grin_client import GRINClient
from services.grin.util import chunk
import argparse

logger = create_log(__name__)

def main(batch_limit=1000):
    grin_client = GRINClient()
    with DBManager() as db_manager:
        url = grin_client._url(
            f"_all_books?book_state=NEW&book_state=PREVIOUSLY_DOWNLOADED&format=text"
        )
        print(f"Scraping url: {url}")

        response = grin_client.session.request("GET", url, timeout=600)
        response.raise_for_status()

        barcodes = response.content.decode("utf8").strip().split("\n")
        if len(barcodes) > 0:
            insert_into_db(
                barcodes=barcodes, db_manager=db_manager, chunk_size=batch_limit
            )
        else:
            logger.info("No record found")


def insert_into_db(barcodes: List[str], db_manager: DBManager, chunk_size: int):
    logger.info(f"Processing {len(barcodes)} barcodes")

    for chunked_barcodes in chunk(iter(barcodes), chunk_size):
        new_records: List[Record] = []
        for barcode in chunked_barcodes:
            existing_record = (
                db_manager.session.query(Record)
                .filter(Record.source_id == f"{barcode}|grin")
                .first()
            )
            if not existing_record:
                new_records.append(
                    Record(
                        uuid=uuid4(),
                        frbr_status=FRBRStatus.TODO.value,
                        source_id=f"{barcode}|grin",
                        source="grin",
                        grin_status=GRINStatus(
                            barcode=barcode,
                            failed_download=0,
                            state=GRINState.PENDING_CONVERSION.value,
                            date_created=datetime(1991, 8, 25),
                        ),
                    )
                )
        logger.info(f"Inserting {len(new_records)} barcodes into Record")

        try:
            db_manager.session.add_all(new_records)
            db_manager.commit_changes()
        except Exception:
            logger.exception(f"Failed to insert barcodes: {chunked_barcodes}")
            raise

        break
    logger.info("Complete.")


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--batch_limit")
        args = parser.parse_args()
        batch_limit = int(args.batch_limit)

        main(batch_limit)
    except Exception as e:
        logger.exception(e, exc_info=True)
