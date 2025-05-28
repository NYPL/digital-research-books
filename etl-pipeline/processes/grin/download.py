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
import argparse


class GRINDownload:
    def __init__(self, *args, batch_limit=1):
        self.s3_manager = S3Manager()
        self.client = GRINClient()
        self.logger = create_log(__name__)
        self.bucket = (
            "drb-files-limited-production"
            if os.environ.get("ENVIRONMENT", "qa") == "production"
            else "drb-files-limited-qa"
        )
        self.batch_limit = batch_limit

    def runProcess(self, backfill=True):
        with DBManager() as self.db_manager:
            if backfill:
                backfilled_books: List[Record] = self._get_converted_books(backfill)
                self.download_and_upload_books(backfilled_books)

            daily_converted_books: List[Record] = self._get_converted_books()
            self.download_and_upload_books(daily_converted_books)

    def download_and_upload_books(self, books):
        for chunked_books in chunk(iter(books), self.batch_limit):
            successfully_processed_books: List[str] = []
            for book in chunked_books:
                barcode = book.source_id.split("|")[0]
                file_name = f"{barcode}.tar.gz.gpg"
                s3_key = f"grin/{file_name}"

                try:
                    content = self._download(file_name)
                except:
                    self.logger.exception(f"Error downloading content for {barcode}")
                    book.grin_status.failed_download += 1
                    self.db_manager.commit_changes()
                    continue

                try:
                    self.s3_manager.put_object(
                        object=content,
                        key=s3_key,
                        bucket=self.bucket,
                        storage_class="GLACIER_IR",
                    )
                except Exception as e:
                    self.logger.exception(f"Error uploading to s3 for {barcode}")
                    continue

                # Only update the `state` when download and upload_to_S3 operations succeeded
                book.grin_status.state = GRINState.DOWNLOADED.value
                successfully_processed_books.append(book)

            if len(successfully_processed_books) > 0:
                self._update_states(successfully_processed_books)
                self.logger.info(
                    f"Successfully downloaded and uploaded {len(successfully_processed_books)} books"
                )

    def _update_states(self, books: List[Record]):
        try:
            self.db_manager.bulk_save_objects(books)
        except:
            self.db_manager.session.rollback()
            self.logger.exception(f"Error updating the GRINStatus table for {books}")

    def _download(self, file_name: str):
        response = self.client.download(file_name)
        return response

    def _get_converted_books(self, backfill=False):
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
            query = query.limit(self.batch_limit)
        else:
            yesterday = datetime.now() - timedelta(days=1)
            query = query.where(GRINStatus.date_modified >= yesterday)

        books = self.db_manager.session.execute(query).scalars().all()
        return books


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_limit")
    args = parser.parse_args()
    batch_limit = int(args.batch_limit)

    grin_download = GRINDownload(batch_limit=batch_limit)
    grin_download.runProcess()
