from managers import DBManager
from model import GRINStatus, GRINState
from processes.grin.download import GRINDownloadService
import boto3
from botocore.exceptions import ClientError
import pytest
from managers import DBManager

TEST_BUCKET = "drb-grin-files-test"
TEST_BARCODE = "33433000009799"  # barcode with converted status in local test db


@pytest.fixture
def db_manager():
    with DBManager() as manager:
        yield manager


def test_s3_bucket():
    s3 = boto3.client("s3")
    assert test_s3_bucket_read_access(s3, TEST_BUCKET), (
        f"Bucket {TEST_BUCKET} is not readable"
    )
    # assert test_s3_bucket_write_access(s3, TEST_BUCKET), f"Bucket {TEST_BUCKET} is not writable" # uncomment to test write access


def test_s3_bucket_read_access(s3, bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Confirmed read access to bucket '{bucket_name}'.")
        return True
    except ClientError as e:
        print(f"No read access to bucket '{bucket_name}': {e}")
        return False


# uncomment to test write access
# def test_s3_bucket_write_access(s3, bucket_name):
#     test_key = "test-write-access.txt"
#     try:
#         s3.put_object(Bucket=bucket_name, Key=test_key, Body=b"test")
#         print(f"Confirmed write access to bucket '{bucket_name}'.")
#         # s3.delete_object(Bucket=bucket_name, Key=test_key) # uncomment to delete the test file
#         return True
#     except ClientError as e:
#         print(f"No write access to bucket '{bucket_name}': {e}")
#         return False


def test_download_barcode(db_manager):
    # confirm barcode exists and has converted status in db
    grin_status = db_manager.session.get(GRINStatus, TEST_BARCODE)
    assert grin_status is not None, f"Barcode {TEST_BARCODE} not found in database."
    assert grin_status.state == GRINState.CONVERTED.value, (
        f"Barcode {TEST_BARCODE} does not have CONVERTED status. Actual status: {grin_status.state}"
    )

    # confirm bucket exists
    service = GRINDownloadService(TEST_BUCKET)
    assert service.bucket == TEST_BUCKET, (
        f"Expected bucket name '{TEST_BUCKET}', but got {service.bucket}"
    )

    download_result = service.download_barcode(TEST_BARCODE)
    print(f"Download result: {download_result}")
