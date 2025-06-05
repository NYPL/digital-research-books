from .grin_client import GRINClient
from managers import DBManager, S3Manager
from model import GRINStatus, GRINState
import logging
import os
import argparse

class GRINDownload:
    def __init__(self, barcode):
        self.grin_client = GRINClient()
        self.logger = logging.getLogger()
        self.s3_manager = S3Manager()
        self.bucket = (
            "drb-files-limited-production"
            if os.environ.get("ENVIRONMENT", "qa") == "production"
            else "drb-files-limited-qa"
        )
        self.barcode = str(barcode)

    def run_process(self):
        with DBManager() as self.db_manager:
            self.download_and_upload_book()
            # self.unpack_and_upload_ocr_files()
    
    def download_and_upload_book(self):
        grin_status = self.db_manager.session.get(GRINStatus, self.barcode)
        file_name = f"{self.barcode}.tar.gz.gpg"
        s3_key = f"grin/{self.barcode}/{file_name}"

        try:
            content = self.grin_client.download(file_name)
        except:
            self.logger.exception(f"Error downloading content for {self.barcode}")
            grin_status.failed_download += 1
            self.db_manager.commit_changes()
            return

        try:
            self.s3_manager.put_object(
                object=content,
                key=s3_key,
                bucket=self.bucket,
                storage_class="GLACIER_IR",
            )
        except Exception as e:
            self.logger.exception(f"Error uploading to s3 for {self.barcode}")
            return

        grin_status.state = GRINState.DOWNLOADED.value
        self.db_manager.commit_changes()
        return file_name
    
    # def unpack_and_upload_ocr_files(self):


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--barcode")
    args = parser.parse_args()
    barcode = int(args.barcode)

    grin_download = GRINDownload(barcode)
    grin_download.run_process()