import os
import requests
from urllib.parse import quote_plus

from digital_assets import get_stored_file_url
from logger import create_log
from managers import S3Manager
from model import Record

logger = create_log(__name__)


class RecordFileSaver:
    WEBPUB_CONVERSION_BASE_URL = 'https://epub-to-webpub.vercel.app'

    def __init__(self, storage_manager: S3Manager):
        self.storage_manager = storage_manager
        self.file_bucket = os.environ.get('FILE_BUCKET')

    def save_record_files(self, record: Record):
        self.storage_manager.store_pdf_manifest(record=record, bucket_name=self.file_bucket)

        # TODO: look at has_part and save files

    def store_file(self, file_url: str, file_path: str):
        try:
            file_contents = self.get_file_contents(file_url)
            self.storage_manager.put_object(file_contents, file_path, self.file_bucket)
            del file_contents

            if '.epub' in file_path:
                file_root = '.'.join(file_path.split('.')[:-1])

                web_pub_manifest = self.generate_webpub(file_root)
                self.storage_manager.put_object(web_pub_manifest, f'{file_root}/manifest.json', self.file_bucket)

            logger.info(f'Stored file {file_path} from {file_url}')
        except Exception as e:
            logger.exception(f'Failed to store file {file_path} from {file_url}')
            raise e

    def get_file_contents(self, file_url: str):
        try:
            file_url_response = requests.get(
                file_url,
                stream=True,
                timeout=15,
                headers={ 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)' }
            )

            file_url_response.raise_for_status()

            file_contents = bytes()

            for byte_chunk in file_url_response.iter_content(1024 * 250):
                file_contents += byte_chunk

            return file_contents
        except Exception as e:
            logger.exception(f'Failed to get file from {file_url}')
            raise e
    
    def generate_webpub(self, file_root: str):
        file_path = get_stored_file_url(storage_name=self.file_bucket, file_path=f'{file_root}/META-INF/container.xml')
        webpub_conversion_url = f'{RecordFileSaver.WEBPUB_CONVERSION_BASE_URL}/api/{quote_plus(file_path)}'

        try:
            webpub_response = requests.get(webpub_conversion_url, timeout=15)

            webpub_response.raise_for_status()

            return webpub_response.content
        except Exception as e:
            logger.exception(f'Failed to generate webpub for {file_root}')
            raise e
