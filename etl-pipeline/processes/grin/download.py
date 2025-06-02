from .grin_client import GRINClient
import logging

class GRINDownload:
    def init(self):
        self.grin_client = GRINClient()
        self.logger = logging.getLogger()
    def download_book(self, barcode):
        file_name = f"{barcode}.tar.gz.gpg"
        s3_key = f"grin/{file_name}"
        try:
            content = self.grin_client._download(file_name)
        except:
            self.logger.exception(f"Error downloading content for {barcode}")
            # book.grin_status.failed_download += 1
            # self.db_manager.commit_changes()