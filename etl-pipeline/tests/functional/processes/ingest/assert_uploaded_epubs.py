from model import Record
from managers.s3 import S3Manager


def assert_uploaded_epubs(records: list[Record]):
    s3_manager = S3Manager()

    for record in records:
        epub_part = next(
            (part for part in record.parts if part.file_type == "application/epub+zip"),
            None,
        )

        if epub_part and "epub" in epub_part.url:
            epub_head_response = s3_manager.client.head_object(
                Key=epub_part.file_key, Bucket=epub_part.file_bucket
            )

            assert epub_head_response is not None
