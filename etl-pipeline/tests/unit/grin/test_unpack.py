import os
import tarfile
from datetime import datetime, timedelta, timezone
import pytest
from botocore.exceptions import ClientError

from processes.grin.unpack import GRINUnpackService


@pytest.fixture
def mock_unpack_service(mocker):
    mock_download_service_class = mocker.patch(
        "processes.grin.unpack.GRINDownloadService"
    )
    mock_s3_manager_class = mocker.patch("processes.grin.unpack.S3Manager")

    mock_s3_client = mock_s3_manager_class.return_value.client
    mock_download_service = mock_download_service_class.return_value

    bucket = "test-bucket"
    service = GRINUnpackService(bucket)

    return service, mock_s3_client, mock_download_service


def test_unpack_barcode_package_success(mock_unpack_service, mocker):
    service, mock_s3_client, mock_download_service = mock_unpack_service

    barcode = "12345"
    ocr_dir = f"grin/{barcode}/"
    ocr_package_name = f"{barcode}.tar.gz.gpg"
    decrypted_package_path = f"/tmp/decrypted/{barcode}.tar.gz"

    real_join = os.path.join

    def mock_join(*args):
        return (
            "/mocked/temp/path/file.tar.gz.gpg"
            if args[1] == ocr_package_name
            else real_join(*args)
        )

    mocker.patch("processes.grin.unpack.os.path.join", side_effect=mock_join)

    mock_download_service.decrypt_ocr_package.return_value = decrypted_package_path

    mock_member_1 = mocker.MagicMock(spec=tarfile.TarInfo, isfile=lambda: True)
    mock_member_1.name = "file1.txt"
    mock_member_2 = mocker.MagicMock(spec=tarfile.TarInfo, isfile=lambda: True)
    mock_member_2.name = "file2.jpg"

    mock_tarfile_open = mocker.patch("processes.grin.unpack.tarfile.open")

    mock_tar_obj = mocker.MagicMock()
    mock_tar_obj.__enter__.return_value = mock_tar_obj
    mock_tar_obj.__iter__.return_value = [mock_member_1, mock_member_2]

    mock_tar_obj.extractfile.side_effect = [
        mocker.mock_open(read_data=b"content of file 1").return_value,
        mocker.mock_open(read_data=b"content of file 2").return_value,
    ]

    mock_tarfile_open.return_value = mock_tar_obj

    mock_executor = mocker.patch("processes.grin.unpack.executor")

    mock_s3_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "head_object"
    )

    unpacked_files = service.unpack_barcode_package(barcode)

    mock_s3_client.download_file.assert_called_once_with(
        Bucket=service.bucket,
        Key=f"{ocr_dir}{ocr_package_name}",
        Filename="/mocked/temp/path/file.tar.gz.gpg",
    )
    mock_download_service.decrypt_ocr_package.assert_called_once_with(
        barcode, mocker.ANY, mocker.ANY
    )
    mock_tarfile_open.assert_called_once_with(decrypted_package_path, mode="r|*")
    assert mock_executor.submit.call_count == 2

    expected_calls = [
        mocker.call(
            service._upload_file_to_s3, ocr_dir, "file1.txt", b"content of file 1"
        ),
        mocker.call(
            service._upload_file_to_s3, ocr_dir, "file2.jpg", b"content of file 2"
        ),
    ]
    mock_executor.submit.assert_has_calls(expected_calls, any_order=True)

    expected_files = {
        "file1.txt": b"content of file 1",
        "file2.jpg": b"content of file 2",
    }
    assert unpacked_files == expected_files


def test_unpack_barcode_package_tar_error(mock_unpack_service, mocker):
    service, _, mock_download_service = mock_unpack_service
    barcode = "12345"

    mocker.patch("processes.grin.unpack.tempfile.TemporaryDirectory")
    mock_download_service.decrypt_ocr_package.return_value = (
        "/tmp/decrypted/file.tar.gz"
    )

    mocker.patch(
        "processes.grin.unpack.tarfile.open",
        side_effect=tarfile.TarError("Corrupt file"),
    )

    with pytest.raises(Exception, match="Failed to unpack OCR package for 12345"):
        service.unpack_barcode_package(barcode)


def test_upload_file_to_s3_success(mock_unpack_service):
    service, mock_s3_client, _ = mock_unpack_service

    mock_s3_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}}, "head_object"
    )

    file_data = b"test data"
    service._upload_file_to_s3("test_dir/", "test_file.txt", file_data)

    mock_s3_client.head_object.assert_called_once_with(
        Bucket=service.bucket, Key="test_dir/test_file.txt"
    )
    mock_s3_client.put_object.assert_called_once()

    args, kwargs = mock_s3_client.put_object.call_args
    assert kwargs["Body"] == file_data
    assert kwargs["Bucket"] == service.bucket
    assert kwargs["Key"] == "test_dir/test_file.txt"
    assert kwargs["Tagging"] == "retention=7days"


def test_upload_file_to_s3_skip_existing(mock_unpack_service):
    service, mock_s3_client, _ = mock_unpack_service

    mock_s3_client.head_object.return_value = {}

    file_data = b"test data"
    service._upload_file_to_s3("test_dir/", "test_file.txt", file_data)

    mock_s3_client.head_object.assert_called_once()
    mock_s3_client.put_object.assert_not_called()


def test_upload_file_to_s3_client_error(mock_unpack_service):
    service, mock_s3_client, _ = mock_unpack_service

    mock_s3_client.head_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}, "head_object"
    )

    with pytest.raises(ClientError):
        service._upload_file_to_s3("test_dir/", "test_file.txt", b"test data")
