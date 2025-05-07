import io
import boto3
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2.service_account import Credentials
import socket

# Please install the requirements with this command: pip install boto3 google-api-python-client google-auth

s3 = boto3.client("s3")

S3_BUCKET = "drb-unprocessed-partner-data"
SERVICE_ACCOUNT_FILE = "service-account.json"
FOLDER_ID = "1Cv0qF5510ZlPp--emRednPoazEKxyW8y"


def create_drive_service(service_account_file):
    scopes = ["https://www.googleapis.com/auth/drive"]
    socket.setdefaulttimeout(300)
    credentials = Credentials.from_service_account_file(
        service_account_file, scopes=scopes
    )

    return build("drive", "v3", credentials=credentials)


def upload_to_google_drive(file_name, file_data):
    drive_service = create_drive_service(service_account_file=SERVICE_ACCOUNT_FILE)
    parent_folder_id = None

    if "/" in file_name:
        folder_name, file_name = file_name.split("/")

        folder = (
            drive_service.files()
            .create(
                body={
                    "name": folder_name,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [FOLDER_ID],
                },
                supportsAllDrives=True,
                fields="id",
            )
            .execute()
        )

        parent_folder_id = folder.get("id")

    file = (
        drive_service.files()
        .create(
            body={
                "name": file_name,
                "parents": [parent_folder_id] if parent_folder_id else [FOLDER_ID],
            },
            media_body=MediaIoBaseUpload(
                io.BytesIO(file_data), mimetype="application/octet-stream"
            ),
            supportsAllDrives=True,
            fields="id",
        )
        .execute()
    )

    return file.get("id")


def move_s3_files_to_google_drive(s3_bucket):
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=s3_bucket)

    upload_count = 0

    for page in page_iterator:
        if "Contents" not in page:
            continue

        for object in page["Contents"]:
            file_key = object["Key"]

            s3_object = s3.get_object(Bucket=s3_bucket, Key=file_key)
            file_data = s3_object["Body"].read()

            try:
                if file_key.startswith("IA_NYPL/"):
                    file_key = file_key.removeprefix("IA_NYPL/")
                if file_key.startswith("batch_2/"):
                    file_key = file_key.removeprefix("batch_2/")

                upload_to_google_drive(file_key, file_data)
                upload_count += 1

                print(f"Uploaded {upload_count} files", end="\r")
            except Exception as e:
                print(f"Failed to upload {file_key} to Google Drive: {e}")

    print(f"Uploaded {upload_count} files")


if __name__ == "__main__":
    move_s3_files_to_google_drive(S3_BUCKET)
