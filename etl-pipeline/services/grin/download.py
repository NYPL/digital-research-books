# Script run daily to download GRIN books that were converted in the past day

from datetime import datetime, timedelta
from typing import List
from model import GRINState, GRINStatus, Record
from managers import DBManager, S3Manager
from logger import create_log
from sqlalchemy import select, desc
from .util import chunk
from .grin_client import GRINClient
import os

S3_BUCKET = (
    "drb-files-limited-production"
    if os.environ.get("ENVIRONMENT", "qa") == "production"
    else "drb-files-limited-qa"
)
BATCH_SIZE_LIMIT = 1000


class GRINDownload:
    def __init__(self):
        self.s3_manager = S3Manager()
        self.client = GRINClient()
        self.logger = create_log(__name__)

    def run_process(self, batch_size=BATCH_SIZE_LIMIT, backfill=False):
        with DBManager() as self.db_manager:
            if backfill:
                backfilled_books: List[Record] = self._get_converted_books(
                    batch_size, backfill
                )
                self.download_and_upload_books(backfilled_books, batch_size)

            daily_converted_books: List[Record] = self._get_converted_books()
            self.download_and_upload_books(daily_converted_books, batch_size)

    def download_and_upload_books(self, books, batch_size):
        for chunked_books in chunk(iter(books), batch_size):
            successfully_processed_books: List[str] = []
            for book in chunked_books:
                barcode = book.source_id.split("|")[0]
                file_name = f"{barcode}.tar.gz.gpg"
                s3_key = f"grin/{file_name}"

                try:
                    content = self._download(file_name)
                except:
                    self.logger.exception(f"Error downloading content for {book}")
                    book.grin_status.failed_download += 1
                    self.db_manager.commit_changes()
                    continue

                try:
                    self.s3_manager.put_object(
                        object=content,
                        key="grin/" + s3_key,
                        bucket=S3_BUCKET,
                        storage_class="GLACIER_IR",
                    )
                except Exception as e:
                    self.logger.exception(f"Error uploading to s3 for {book}")
                    continue

                # Only update the `state` when download and upload_to_S3 operations succeeded
                book.grin_status.state = GRINState.DOWNLOADED.value
                successfully_processed_books.append(book)

            if len(successfully_processed_books) > 0:
                self._update_states(successfully_processed_books)

    def _update_states(self, books: List[Record]):
        try:
            self.db_manager.bulk_save_objects(books)
        except:
            self.db_manager.session.rollback()
            self.logger.exception(f"Error updating the GRINStatus table for {books}")

    def _download(self, file_name: str):
        response = self.client.download(file_name)
        return response

    def _get_converted_books(self, batch_size=None, backfill=False):
        """Should return the DB objects so that we can update the state directly"""
        query = (
            select(Record)
            .join(Record.grin_status)
            .where(GRINStatus.state == GRINState.CONVERTED.value)
            .order_by(desc(Record.date_modified))
        )

        if backfill:
            query = query.where(
                GRINStatus.date_created <= GRINStatus.backfill_timestamp()
            )
            query = query.limit(batch_size)
        else:
            yesterday = datetime.now() - timedelta(days=1)
            query = query.where(GRINStatus.date_modified >= yesterday)

        books = self.db_manager.session.execute(query).scalars().all()
        return books


if __name__ == "__main__":
    grin_download = GRINDownload()
    grin_download.run_process(backfill=True)
