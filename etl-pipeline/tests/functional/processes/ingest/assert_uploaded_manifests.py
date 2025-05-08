from model import Record
from managers.s3 import S3Manager


def assert_uploaded_manifests(records: list[Record]):
    s3_manager = S3Manager()

    for record in records:
        manifest_part = next(
            (
                part
                for part in record.parts
                if part.file_type == "application/webpub+json"
            ),
            None,
        )

        if manifest_part and "epubs" not in manifest_part.url:
            manifest_head_response = s3_manager.client.head_object(
                Key=manifest_part.file_key, Bucket=manifest_part.file_bucket
            )

            assert manifest_head_response is not None
