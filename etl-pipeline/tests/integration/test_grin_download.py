import pytest
import os
import boto3
from managers import DBManager
from model import GRINStatus, GRINState
from processes.grin.download import GRINDownloadService
from botocore.exceptions import ClientError
from logger import create_log
from datetime import datetime

logger = create_log(__name__)

TEST_BARCODE = "33433000009799"  # barcode with converted status in local test db


@pytest.fixture
def db_manager():
    with DBManager() as manager:
        yield manager


@pytest.fixture
def test_bucket():
    return os.environ["PRIVATE_FILE_BUCKET"]


@pytest.fixture
def grin_download_service(test_bucket):
    return GRINDownloadService(test_bucket)


def _get_grin_status(db_manager, barcode):
    grin_status = db_manager.session.get(GRINStatus, barcode)
    return grin_status


def _set_grin_status(db_manager, barcode, state):
    grin_status = _get_grin_status(db_manager, barcode)
    if grin_status is None:
        grin_status = GRINStatus(
            barcode=barcode,
            failed_download=0,
            state=state.value,
            date_created=datetime(1991, 8, 25),
        )
        db_manager.session.add(grin_status)
    else:
        grin_status.state = state.value
    db_manager.session.commit()
    return _get_grin_status(db_manager, barcode)


def test_s3_bucket(grin_download_service, test_bucket):
    s3 = boto3.client(
        "s3", endpoint_url="http://localhost:4566"
    )  # use LocalStack endpoint
    assert grin_download_service.bucket == test_bucket, (
        f"Bucket '{test_bucket}' does not exist and instead received {grin_download_service.bucket}"
    )
    assert _localstack_bucket_read_access(s3, test_bucket), (
        f"Bucket {test_bucket} is not readable"
    )
    assert _localstack_bucket_write_access(s3, test_bucket), (
        f"Bucket {test_bucket} is not writable"
    )


def _localstack_bucket_read_access(s3, bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"Confirmed read access to bucket '{bucket_name}'.")
        return True
    except ClientError as e:
        logger.error(f"No read access to bucket '{bucket_name}': {e}")
        return False


def _localstack_bucket_write_access(s3, bucket_name):
    test_key = "test-write-access.txt"
    try:
        s3.put_object(Bucket=bucket_name, Key=test_key, Body=b"test")
        logger.info(f"Confirmed write access to bucket '{bucket_name}'.")
        # s3.delete_object(Bucket=bucket_name, Key=test_key) # deletes the test file
        return True
    except ClientError as e:
        logger.error(f"No write access to bucket '{bucket_name}': {e}")
        return False


def test_download_process(grin_download_service, db_manager):
    # confirm barcode has CONVERTED state before proceeding with download
    if _get_grin_status(db_manager, TEST_BARCODE) is None:
        _set_grin_status(db_manager, TEST_BARCODE, GRINState.CONVERTED)
    grin_status = _get_grin_status(db_manager, TEST_BARCODE)
    assert grin_status.state == GRINState.CONVERTED.value, (
        f"Barcode {TEST_BARCODE} does not have CONVERTED status. Actual status: {grin_status.state}"
    )

    ocr_dir, mets_file_path = grin_download_service.download_barcode(TEST_BARCODE)
    assert os.path.exists(ocr_dir), f"OCR directory {ocr_dir} does not exist."
    assert os.path.exists(mets_file_path), f"METS file {mets_file_path} does not exist."

    # confirm barcode has DOWNLOADED state
    grin_status = _get_grin_status(db_manager, TEST_BARCODE)
    assert grin_status.state == GRINState.DOWNLOADED.value, (
        f"Barcode {TEST_BARCODE} does not have DOWNLOADED status. Actual status: {grin_status.state}"
    )
