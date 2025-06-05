from .grin_client import GRINClient
from managers import DBManager, S3Manager
from model import GRINStatus, GRINState
import gnupg
import logging
import os
import base64
import io
import argparse
import tarfile


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
            file_content = self.download_and_upload_book()

            self.unpack_and_upload_ocr_files(file_content)

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

        # try:
        #     self.s3_manager.put_object(
        #         object=content,
        #         key=s3_key,
        #         bucket=self.bucket,
        #         storage_class="GLACIER_IR",
        #     )
        # except Exception as e:
        #     self.logger.exception(f"Error uploading to s3 for {self.barcode}")
        #     return

        grin_status.state = GRINState.DOWNLOADED.value
        self.db_manager.commit_changes()
        return content

    def unpack_and_upload_ocr_files(self, file_content):
        gpg = gnupg.GPG()
        decrypted_content = gpg.decrypt(
            file_content,
            always_trust=True,
            # passphrase=grin_access_key,
        )
        print("Decryption successful!")

        tar_stream_data = io.BytesIO(decrypted_content.data)
        try:
            with tarfile.open(fileobj=tar_stream_data, mode="r|*") as tar_file:
                for file in tar_file:
                    self.bucket.upload_file(
                        io.BytesIO(tar_file.extractfile(file).read()),
                        key=f"grin/{self.barcode}/{file.name}",
                    )
        except tarfile.StreamError as e:
            print(f"Error reading stream: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--barcode")
    args = parser.parse_args()
    barcode = int(args.barcode)

    grin_download = GRINDownload(barcode)
    grin_download.run_process()
