import os


def get_stored_file_url(storage_name: str, file_path: str):
    if os.environ.get("ENVIRONMENT") == "local":
        return f"{os.environ.get('S3_ENDPOINT_URL')}/{storage_name}/{file_path}"

    return f"https://{storage_name}.s3.amazonaws.com/{file_path}"
