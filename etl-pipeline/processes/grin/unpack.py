import os
import tempfile
import tarfile

from processes.grin.download import GRINDownloadService
from logger import create_log

logger = create_log(__name__)


class GRINUnpackService:
    def __init__(self, bucket):
        self.bucket = bucket
        self.download_service = GRINDownloadService(bucket)

    def unpack_barcode_package(self, barcode):
        barcode = str(barcode)
        ocr_dir = f"grin/{barcode}/"
        ocr_package_name = f"{barcode}.tar.gz.gpg"

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_ocr_package = os.path.join(tmp_dir, ocr_package_name)
            logger.info(f"Downloading {ocr_package_name} from S3 bucket {self.bucket}")
            self.download_service.s3_manager.client.download_file(
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
                            files[member.name] = file_obj.read()
                logger.info(f"Unpacked {len(files)} files for barcode {barcode}")
            except tarfile.TarError as e:
                logger.error(f"Error unpacking OCR package for {barcode}: {e}")
                raise Exception(f"Failed to unpack OCR package for {barcode}")

            return files
