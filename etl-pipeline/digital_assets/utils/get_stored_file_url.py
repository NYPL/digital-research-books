import os

from utils.common import require_env


def get_stored_file_url(storage_name: str, file_path: str):
    if os.environ.get("ENVIRONMENT") == "local":
        return f"{require_env('S3_ENDPOINT_URL')}/{storage_name}/{file_path}"

    return f"https://{storage_name}.s3.amazonaws.com/{file_path}"
