import os
import requests
from urllib.parse import quote_plus

from digital_assets import get_stored_file_url
from logger import create_log
from managers import DBManager, S3Manager
from model import Record, RecordState, Part
from services.google_drive_service import GoogleDriveService

logger = create_log(__name__)


class RecordFileSaver:
    WEBPUB_CONVERSION_BASE_URL = "https://epub-to-webpub.vercel.app"

    def __init__(self, db_manager: DBManager, storage_manager: S3Manager):
        self.db_manager = db_manager
        self.storage_manager = storage_manager
        self.file_bucket = os.environ["FILE_BUCKET"]
        self.limited_file_bucket = f"drb-files-limited-{os.environ['ENVIRONMENT']}"
        self.drive_service = GoogleDriveService()

    def save_record_files(self, record: Record) -> Record:
        self.storage_manager.store_pdf_manifest(
            record=record, bucket_name=self.file_bucket
        )
        files_to_store = (
            part
            for part in record.parts
            if part.source_url and part.file_bucket and part.file_key
        )

        for file_to_store in files_to_store:
            self.store_file(part=file_to_store)

        record.state = RecordState.FILES_SAVED.value

        self.db_manager.session.commit()
        self.db_manager.session.refresh(record)

        return record

    def store_file(self, part: Part):
        try:
            if "drive.google.com" in part.source_url:
                self._store_file_from_drive(part)
            elif part.source_file_key and part.source_file_bucket:
                self._copy_file(part)
            else:
                file_contents = self.get_file_contents(part.source_url)
                self.storage_manager.put_object(
                    file_contents, part.file_key, part.file_bucket
                )
                del file_contents

                if ".epub" in part.file_key:
                    file_root = ".".join(part.file_key.split(".")[:-1])

                    web_pub_manifest = self.generate_webpub(file_root)
                    self.storage_manager.put_object(
                        web_pub_manifest, f"{file_root}/manifest.json", self.file_bucket
                    )

            logger.info(f"Stored file at {part.url} from {part.source_url}")
        except Exception as e:
            logger.exception(
                f"Failed to store file {part.file_key} from {part.source_url}"
            )
            raise e

    def _store_file_from_drive(self, part: Part):
        file_id = f"{self.drive_service.id_from_url(part.source_url)}"
        file = self.drive_service.get_drive_file(file_id)

        if not file:
            raise Exception(f"Unable to get file for: {part}")

        file_permissions = (
            {}
            if part.file_bucket == self.limited_file_bucket
            else {"ACL": "public-read"}
        )
        self.storage_manager.client.upload_fileobj(
            file, part.file_bucket, part.file_key, file_permissions
        )

    def _copy_file(self, part: Part):
        source_bucket_key = {
            "Bucket": part.source_file_bucket,
            "Key": part.source_file_key,
        }
        extra_args = (
            {"ACL": "public-read"} if part.file_bucket == self.file_bucket else {}
        )

        self.storage_manager.client.copy(
            source_bucket_key, part.file_bucket, part.file_key, extra_args
        )

    def get_file_contents(self, file_url: str):
        try:
            file_url_response = requests.get(
                file_url,
                stream=True,
                timeout=15,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)"
                },
            )

            file_url_response.raise_for_status()

            file_contents = bytes()

            for byte_chunk in file_url_response.iter_content(1024 * 250):
                file_contents += byte_chunk

            return file_contents
        except Exception as e:
            logger.exception(f"Failed to get file from {file_url}")
            raise e

    def generate_webpub(self, file_root: str):
        file_path = get_stored_file_url(
            storage_name=self.file_bucket,
            file_path=f"{file_root}/META-INF/container.xml",
        )
        webpub_conversion_url = (
            f"{RecordFileSaver.WEBPUB_CONVERSION_BASE_URL}/api/{quote_plus(file_path)}"
        )

        try:
            webpub_response = requests.get(webpub_conversion_url, timeout=15)

            webpub_response.raise_for_status()

            return webpub_response.content
        except Exception as e:
            logger.exception(f"Failed to generate webpub for {file_root}")
            raise e
