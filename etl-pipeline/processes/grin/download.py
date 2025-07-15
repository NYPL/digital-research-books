import io
import os
import tarfile
import tempfile
import gnupg
import shutil

from .grin_client import GRINClient
from managers import DBManager, S3Manager
from model import GRINStatus, GRINState
from services.ssm_service import SSMService
from logger import create_log

logger = create_log(__name__)


class GRINDownloadService:
    def __init__(self, bucket):
        self.grin_client = GRINClient()
        self.s3_manager = S3Manager()
        self.ssm_service = SSMService()
        self.bucket = bucket

    def download_barcode(self, barcode):
        barcode = str(barcode)
        ocr_dir = f"grin/{barcode}/"

        with DBManager() as self.db_manager, tempfile.TemporaryDirectory() as tmp_dir:
            grin_status = self.db_manager.session.get(GRINStatus, barcode)

            tmp_ocr_package = self.download_ocr_package(barcode, grin_status, tmp_dir)
            self.upload_ocr_package(barcode, ocr_dir, tmp_ocr_package, grin_status)
            self.upload_unpacked_ocr_files(barcode, ocr_dir, tmp_ocr_package, tmp_dir)

            mets_file = ocr_dir + f"NYPL_{barcode}.xml"
            return ocr_dir, mets_file

    def download_ocr_package(self, barcode, grin_status: GRINStatus, tmp_dir):
        ocr_package_name = f"{barcode}.tar.gz.gpg"
        tmp_ocr_package = os.path.join(tmp_dir, ocr_package_name)

        try:
            response = self.grin_client.download(ocr_package_name, stream=True)
            logger.info(f"Downloading {barcode} OCR package from GRIN")

            with response:
                response.raise_for_status()

                with open(tmp_ocr_package, "wb") as ocr_package:
                    shutil.copyfileobj(response.raw, ocr_package)

            return tmp_ocr_package
        except Exception:
            logger.exception(f"Error downloading OCR package for {barcode}")
            grin_status.failed_download += 1
            self.db_manager.commit_changes()
            raise Exception(f"Failed to download OCR package for {barcode}")

    def upload_ocr_package(self, barcode, ocr_dir, tmp_ocr_package, grin_status: GRINStatus):
        ocr_package_key = ocr_dir + f"{barcode}.tar.gz.gpg"

        try:
            with open(tmp_ocr_package, "rb") as ocr_package:
                self.s3_manager.client.upload_fileobj(
                    Fileobj=ocr_package,
                    Bucket=self.bucket,
                    Key=ocr_package_key,
                    ExtraArgs={"StorageClass": "GLACIER_IR"},
                )
            logger.info(f"Uploading {barcode} OCR package to s3")
        except Exception:
            logger.exception(f"Error uploading OCR package to s3 for {barcode}")
            raise Exception(f"Failed to upload OCR package for {barcode}")

        grin_status.state = GRINState.DOWNLOADED.value
        self.db_manager.commit_changes()

        return ocr_package_key

    def upload_unpacked_ocr_files(self, barcode, ocr_dir, tmp_ocr_package, tmp_dir):
        decrypted_ocr_package = os.path.join(tmp_dir, f"{barcode}.tar.gz")

        logger.info(f"Decrypting {barcode} OCR package")
        gpg = gnupg.GPG()
        with open(tmp_ocr_package, "rb") as encrypted_file:
            decrypt_result = gpg.decrypt_file(
                encrypted_file,
                passphrase=self.ssm_service.get_parameter("grin-access-key"),
                output=decrypted_ocr_package,
            )

        if not decrypt_result.ok:
            logger.error(
                f"GPG decryption failed for {barcode}: {decrypt_result.status}"
            )
            raise Exception(f"Failed to decrypt OCR package for {barcode}")

        logger.info(f"Unpacking and uploading {barcode} OCR files to s3")
        try:
            with tarfile.open(decrypted_ocr_package, mode="r|*") as tar_file:
                for file in tar_file:
                    file_obj = tar_file.extractfile(file)

                    if file_obj:
                        self.s3_manager.client.upload_fileobj(
                            io.BytesIO(file_obj.read()),
                            Bucket=self.bucket,
                            Key=ocr_dir + file.name,
                        )
        except tarfile.TarError:
            logger.exception(f"Error unpacking OCR package for {barcode}")
            raise Exception(f"Failed to unpack OCR package for {barcode}")
