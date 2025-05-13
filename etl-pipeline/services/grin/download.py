from datetime import datetime
from typing import List
from model import GRINState, GRINStatus, Record
from managers import DBManager, S3Manager
from logger import create_log
from scripts.grin_mirror import GRINClient
from sqlalchemy import select, desc

logger = create_log(__name__)

S3_BUCKET_NAME = "drb-files-qa"
BATCH_SIZE_LIMIT = 1000


class GRINDownload:
    def __init__(self):
        self.s3_client = S3Manager()
        self.db_manager = DBManager()
        self.db_manager.create_session()

        self.client = GRINClient()

    def run_process(self, batch_size=BATCH_SIZE_LIMIT, backfill=False):
        books: List[Record] = self._get_converted_books(batch_size, backfill)
        books_done_processing: List[str] = []

        for book in books:
            barcode = book.source_id.split("|")[0]
            file_name = f"{barcode}.tar.gz.gpg"
            s3_key = f"grin/{file_name}"

            try:
                content = self._download(file_name)
            except:
                logger.exception(f"Error downloading content for {book}")
                book.grin_status.failed_download += 1
                self.db_manager.commit_changes()
                continue

            try:
                self.s3_client.put_object(
                    object=content,
                    s3_key=s3_key,
                    bucket=S3_BUCKET_NAME,
                    storage_class="GLACIER_IR",
                )
            except Exception as e:
                logger.exception(f"Error uploading to s3 for {book}")
                continue

            # Only update the `state` when download and upload_to_S3 operations succeeded
            book.grin_status.state = GRINState.DOWNLOADED.value
            books_done_processing.append(book)

        if len(books_done_processing) > 0:
            self._update_states(books_done_processing)

        self.db_manager.session.close()

    def _update_states(self, books: List[Record]):
        try:
            self.db_manager.bulk_save_objects(books)
        except:
            self.db_manager.session.rollback()
            logger.exception(f"Error updating the GRINStatus table for {books}")

    def _download(self, file_name: str):
        response = self.client.download(file_name)
        return response

    def _get_converted_books(self, batch_size: int, backfill: bool):
        """Should return the DB object so that we can update the state directly"""
        query = (
            select(Record)
            .join(Record.grin_status)
            .where(GRINStatus.state == GRINState.CONVERTED.value)
            .order_by(desc(Record.date_modified))
            .limit(batch_size)
        )

        if backfill:
            query = query.where(GRINStatus.date_created <= datetime(1991, 8, 25))

        books = self.db_manager.session.execute(query).scalars().all()
        return books


if __name__ == "__main__":
    # Run the main process
    grin_download = GRINDownload()
    grin_download.run_process()

    # Run the backfill process
    grin_download.run_process(backfill=True)
