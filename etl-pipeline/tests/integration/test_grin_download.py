import os
from managers import DBManager
from model import GRINStatus, GRINState
from processes.grin.download import GRINDownloadService
import boto3
from botocore.exceptions import ClientError
import pytest
from logger import create_log

logger = create_log(__name__)

TEST_BUCKET = "drb-grin-files-test"
TEST_BARCODE = "33433000009799"  # barcode with converted status in local test db


@pytest.fixture
def db_manager():
    with DBManager() as manager:
        yield manager


@pytest.fixture
def grin_download_service():
    return GRINDownloadService(TEST_BUCKET)


# def set_grin_status(db_manager, barcode, state):
#     grin_status = db_manager.session.get(GRINStatus, barcode)
#     if grin_status is None:
#         grin_status = GRINStatus(
#             barcode=barcode,
#             state=state.value,
#         )
#         db_manager.session.add(grin_status)
#     else:
#         grin_status.state = state.value
#     db_manager.session.commit()
#     return grin_status


def test_s3_bucket(grin_download_service):
    s3 = boto3.client("s3")
    assert grin_download_service.bucket == TEST_BUCKET, (
        f"Bucket '{TEST_BUCKET}' does not exist and instead received {grin_download_service.bucket}"
    )
    assert s3_bucket_read_access(s3, TEST_BUCKET), (
        f"Bucket {TEST_BUCKET} is not readable"
    )
    # assert s3_bucket_write_access(s3, TEST_BUCKET), (
    #     f"Bucket {TEST_BUCKET} is not writable"
    # )


def s3_bucket_read_access(s3, bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"Confirmed read access to bucket '{bucket_name}'.")
        return True
    except ClientError as e:
        logger.error(f"No read access to bucket '{bucket_name}': {e}")
        return False


# def s3_bucket_write_access(s3, bucket_name):
#     test_key = "test-write-access.txt"
#     try:
#         s3.put_object(Bucket=bucket_name, Key=test_key, Body=b"test")
#         logger.info(f"Confirmed write access to bucket '{bucket_name}'.")
#         # s3.delete_object(Bucket=bucket_name, Key=test_key) # deletes the test file
#         return True
#     except ClientError as e:
#         logger.error(f"No write access to bucket '{bucket_name}': {e}")
#         return False


def test_barcode_converted(db_manager):
    # insert test record if not present
    grin_status = db_manager.session.get(GRINStatus, TEST_BARCODE)
    if grin_status is None:
        grin_status = GRINStatus(
            barcode=TEST_BARCODE,
            state=GRINState.CONVERTED.value,
        )
        db_manager.session.add(grin_status)
        db_manager.session.commit()

    # confirm barcode exists and has converted status in db
    grin_status = db_manager.session.get(GRINStatus, TEST_BARCODE)
    assert grin_status is not None, f"Barcode {TEST_BARCODE} not found in database."
    assert grin_status.state == GRINState.CONVERTED.value, (
        f"Barcode {TEST_BARCODE} does not have CONVERTED status. Actual status: {grin_status.state}"
    )


def test_download_process(grin_download_service):
    bucket = os.environ["PRIVATE_FILE_BUCKET"]
    grin_download_service = GRINDownloadService(bucket=bucket)
    ocr_dir, mets_file_path = grin_download_service.download_barcode(TEST_BARCODE)
    logger.info(f"OCR dir: {ocr_dir}, METS file: {mets_file_path}")


def test_barcode_downloaded(db_manager):
    # confirm barcode exists and has downloaded status in db
    grin_status = db_manager.session.get(GRINStatus, TEST_BARCODE)
    assert grin_status is not None, f"Barcode {TEST_BARCODE} not found in database."
    assert grin_status.state == GRINState.DOWNLOADED.value, (
        f"Barcode {TEST_BARCODE} does not have DOWNLOADED status. Actual status: {grin_status.state}"
    )
