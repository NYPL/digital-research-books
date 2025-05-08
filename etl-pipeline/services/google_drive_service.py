from io import BytesIO
import json
import os
from typing import Optional
from urllib.parse import urlparse, parse_qs
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from services.ssm_service import SSMService


from logger import create_log

logger = create_log(__name__)


class GoogleDriveService:
    def __init__(self):
        ssm_service = SSMService()

        SERVICE_ACCOUNT_FILE = ssm_service.get_parameter("google-drive-service-key")

        service_account_info = json.loads(SERVICE_ACCOUNT_FILE)
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive.metadata",
        ]
        credentials = Credentials.from_service_account_info(
            service_account_info, scopes=scopes
        )
        self.drive_service = build("drive", "v3", credentials=credentials)

    def get_drive_file(self, file_id: str) -> Optional[BytesIO]:
        request = self.drive_service.files().get_media(fileId=file_id)
        file = BytesIO()

        try:
            downloader = MediaIoBaseDownload(file, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()

        except HttpError as error:
            logger.warning(
                f"HTTP error occurred when downloading Drive file {file_id}: {error}"
            )
            return None
        except Exception as err:
            logger.exception(f"Error occurred when downloading Drive file {file_id}")
            return None

        return file

    def get_file_metadata(self, file_id: str) -> Optional[str]:
        # supportsAllDrives=True required as of 12/24 despite deprecation
        request = self.drive_service.files().get(fileId=file_id, supportsAllDrives=True)
        metadata = request.execute()
        return metadata

    @staticmethod
    def id_from_url(url: str) -> Optional[str]:
        return parse_qs(urlparse(url).query)["id"][0]
