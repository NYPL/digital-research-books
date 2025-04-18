from model import Record
from managers.s3 import S3Manager


def assert_uploaded_files(records: list[Record]):
    s3_manager = S3Manager()

    for record in records:
        for part in record.parts:
            if part.file_bucket and part.file_key:
                file_head_response = s3_manager.client.head_object(Key=part.file_key, Bucket=part.file_bucket)

                assert file_head_response is not None
