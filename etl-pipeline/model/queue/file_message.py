def get_file_message(file_url: str, bucket_path: str):
    return {"fileData": {"fileURL": file_url, "bucketPath": bucket_path}}
