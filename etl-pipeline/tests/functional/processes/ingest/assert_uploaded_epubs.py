from model import Record
from managers.s3 import S3Manager


def assert_uploaded_epubs(records: list[Record]):
    s3_manager = S3Manager()

    for record in records:
        manifest_part = next((part for part in record.parts if part.file_type == 'application/epub+zip'), None)

        if manifest_part and 'epub' in manifest_part.url:
            manifest_head_response = s3_manager.client.head_object(Key=manifest_part.file_key, Bucket=manifest_part.file_bucket)

            assert manifest_head_response is not None
