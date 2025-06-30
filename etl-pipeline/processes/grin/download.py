from .grin_client import GRINClient
from managers import DBManager, S3Manager, NYPLAPIManager
from model import GRINStatus, GRINState
from services.ssm_service import SSMService
import gnupg
from logger import create_log
import io
import argparse
import tarfile

logger = create_log(__name__)


class GRINDownload:
    def __init__(self, barcode, bucket):
        self.grin_client = GRINClient()
        self.s3_manager = S3Manager()
        self.ssm_service = SSMService()
        self.bucket = bucket
        self.barcode = str(barcode)
        self.ocr_dir = f"grin/{self.barcode}/"

        self.nypl_api_manager = NYPLAPIManager()

    def run_process(self):
        with DBManager() as self.db_manager:
            file_content = self.download_and_upload_book()

            self.unpack_and_upload_ocr_files(file_content)

            mets_file = self.ocr_dir + f"NYPL_{barcode}.xml"

            return self.ocr_dir, mets_file

    def download_and_upload_book(self):
        grin_status = self.db_manager.session.get(GRINStatus, self.barcode)
        file_name = f"{self.barcode}.tar.gz.gpg"
        s3_key = self.ocr_dir + file_name

        try:
            content = self.grin_client.download(file_name)
            logger.info(f"Downloading {barcode} from GRIN")
        except:
            logger.exception(f"Error downloading content for {self.barcode}")
            grin_status.failed_download += 1
            self.db_manager.commit_changes()
            return

        try:
            self.s3_manager.put_object(
                object=content,
                key=s3_key,
                bucket=self.bucket,
                bucket_permissions=None,
                storage_class="GLACIER_IR",
            )
            logger.info(f"Uploading {barcode} TAR to s3")
        except Exception as e:
            logger.exception(f"Error uploading to s3 for {self.barcode}")
            return

        grin_status.state = GRINState.DOWNLOADED.value
        self.db_manager.commit_changes()
        return content

    def unpack_and_upload_ocr_files(self, file_content):
        gpg = gnupg.GPG()
        decrypted_content = gpg.decrypt(
            file_content,
            always_trust=True,
            passphrase=self.ssm_service.get_parameter("grin-access-key"),
        )

        tar_stream_data = io.BytesIO(decrypted_content.data)

        logger.info(f"Unpacking and uploading {barcode} OCR files to s3")
        try:
            with tarfile.open(fileobj=tar_stream_data, mode="r|*") as tar_file:
                for file in tar_file:
                    self.s3_manager.put_object(
                        object=tar_file.extractfile(file).read(),
                        key=self.ocr_dir + f"{file.name}",
                        bucket=self.bucket,
                        bucket_permissions=None,
                    )
        except tarfile.StreamError as e:
            print(f"Error reading stream: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--barcode")
    args = parser.parse_args()
    barcode = int(args.barcode)

    bucket = "drb-files-limited-qa"

    grin_download = GRINDownload(barcode, bucket)
    grin_download.run_process()
