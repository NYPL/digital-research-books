import atexit
import concurrent.futures
import os
import tempfile
import tarfile

from botocore.exceptions import ClientError
from processes.grin.download import GRINDownloadService
from utils.profiler import profile
from managers import S3Manager
from logger import create_log

logger = create_log(__name__)

executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)


def shutdown_executor():
    executor.shutdown(wait=True)


atexit.register(shutdown_executor)


# TODO: Investigate rework to avoid temporary file I/O and handle potential memory leaks.
class GRINUnpackService:
    def __init__(self, bucket):
        self.bucket = bucket
        self.download_service = GRINDownloadService(bucket)
        self.s3_manager = S3Manager()

    @profile(logger=logger)
    def unpack_barcode_package(self, barcode):
        barcode = str(barcode)
        ocr_dir = f"grin/{barcode}/"
        ocr_package_name = f"{barcode}.tar.gz.gpg"

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_ocr_package = os.path.join(tmp_dir, ocr_package_name)
            logger.info(f"Downloading {ocr_package_name} from S3 bucket {self.bucket}")
            self.s3_manager.client.download_file(
                Bucket=self.bucket,
                Key=f"{ocr_dir}{ocr_package_name}",
                Filename=tmp_ocr_package,
            )

            decrypted_ocr_package = self.download_service.decrypt_ocr_package(
                barcode, tmp_ocr_package, tmp_dir
            )

            files = {}
            try:
                with tarfile.open(decrypted_ocr_package, mode="r|*") as tar:
                    for member in tar:
                        if member.isfile():
                            file_obj = tar.extractfile(member)
                            file_data = file_obj.read()
                            files[member.name] = file_data

                            executor.submit(
                                self._upload_file_to_s3,
                                ocr_dir,
                                member.name,
                                file_data,
                            )

                logger.info(
                    f"Unpacked and started uploads for {len(files)} files for barcode {barcode}"
                )
            except tarfile.TarError as e:
                logger.error(f"Error unpacking OCR package for {barcode}: {e}")
                raise Exception(f"Failed to unpack OCR package for {barcode}")

            return files

    def _upload_file_to_s3(self, ocr_dir, file_name, file_data):
        s3_key = f"{ocr_dir}{file_name}"
        try:
            self.s3_manager.client.head_object(Bucket=self.bucket, Key=s3_key)
            logger.info(f"File {s3_key} already exists in S3, skipping upload.")
            return
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.s3_manager.client.put_object(
                    Body=file_data,
                    Bucket=self.bucket,
                    Key=s3_key,
                    Tagging="retention=7days",
                )
                logger.info(f"Uploaded {file_name} to S3.")
            else:
                logger.error(f"Failed to upload {file_name} to S3: {e}")
                raise e
