from botocore.exceptions import ClientError
import pytest

from managers.s3 import S3Manager, S3Error


class TestS3Manager:
    @pytest.fixture
    def test_instance(self, mocker):
        class TestS3Manager(S3Manager):
            def __init__(self):
                self.client = mocker.MagicMock()

        return TestS3Manager()

    def test_put_object_success_epub(self, test_instance: S3Manager, mocker):
        manager_mocks = mocker.patch.multiple(
            S3Manager,
            get_md5_hash=mocker.DEFAULT,
            get_object=mocker.DEFAULT,
            store_epub=mocker.DEFAULT,
        )
        manager_mocks["get_md5_hash"].return_value = "testMd5Hash"
        manager_mocks["get_object"].return_value = None

        test_instance.client.put_object.return_value = "testObjectResponse"

        testResponse = test_instance.put_object("testObj", "testKey.epub", "testBucket")

        assert testResponse == "testObjectResponse"

        manager_mocks["get_md5_hash"].assert_called_once_with("testObj")
        manager_mocks["get_object"].assert_called_once_with(
            "testKey.epub", "testBucket", md5_hash="testMd5Hash"
        )
        manager_mocks["store_epub"].assert_called_once_with(
            "testObj", "testKey.epub", "testBucket"
        )
        test_instance.client.put_object.assert_called_once_with(
            ACL="public-read",
            Body="testObj",
            Bucket="testBucket",
            Key="testKey.epub",
            ContentType="application/epub+zip",
            ContentMD5="testMd5Hash",
            Metadata={"md5Checksum": "testMd5Hash"},
            StorageClass="STANDARD",
        )

    def test_put_object_existing_outdated(self, test_instance: S3Manager, mocker):
        mock_get_object_response = mocker.MagicMock()
        mock_get_object_response.statusCode = 301
        manager_mocks = mocker.patch.multiple(
            S3Manager,
            get_md5_hash=mocker.DEFAULT,
            get_object=mocker.DEFAULT,
            store_epub=mocker.DEFAULT,
        )
        manager_mocks["get_md5_hash"].return_value = "testMd5Hash"
        manager_mocks["get_object"].return_value = mock_get_object_response

        test_instance.client.put_object.return_value = "testObjectResponse"

        test_response = test_instance.put_object(
            "testObj", "testKey.epub", "testBucket"
        )

        assert test_response == "testObjectResponse"

        manager_mocks["get_md5_hash"].assert_called_once_with("testObj")
        manager_mocks["get_object"].assert_called_once_with(
            "testKey.epub", "testBucket", md5_hash="testMd5Hash"
        )
        manager_mocks["store_epub"].assert_called_once_with(
            "testObj", "testKey.epub", "testBucket"
        )
        test_instance.client.put_object.assert_called_once_with(
            ACL="public-read",
            Body="testObj",
            Bucket="testBucket",
            Key="testKey.epub",
            ContentType="application/epub+zip",
            ContentMD5="testMd5Hash",
            Metadata={"md5Checksum": "testMd5Hash"},
            StorageClass="STANDARD",
        )

    def test_put_object_existing_unmodified(self, test_instance: S3Manager, mocker):
        mock_hash = mocker.patch.object(S3Manager, "get_md5_hash")
        mock_hash.return_value = "testMd5Hash"
        mock_get = mocker.patch.object(S3Manager, "get_object")
        mock_get.return_value = {"ResponseMetadata": {"HTTPStatusCode": 304}}

        test_response = test_instance.put_object(
            "testObj", "testKey.epub", "testBucket"
        )

        assert test_response is None

        mock_hash.assert_called_once_with("testObj")
        mock_get.assert_called_once_with(
            "testKey.epub", "testBucket", md5_hash="testMd5Hash"
        )
        test_instance.client.put_object.assert_not_called

    def test_put_object_error(self, test_instance: S3Manager, mocker):
        mock_hash = mocker.patch.object(S3Manager, "get_md5_hash")
        mock_hash.return_value = "testMd5Hash"
        mock_get = mocker.patch.object(S3Manager, "get_object")
        mock_get.return_value = None

        test_instance.client.put_object.side_effect = ClientError({}, "Testing")

        with pytest.raises(S3Error):
            test_instance.put_object("testObj", "testKey", "testBucket")

    def test_store_epub(self, test_instance: S3Manager, mocker):
        mock_epub_zip_component = mocker.MagicMock()
        mock_epub_zip_component.read.side_effect = ["compBytes1", "compBytes2"]

        mock_epub_zip = mocker.MagicMock()
        mock_epub_zip.namelist.return_value = ["comp1", "comp2"]
        mock_epub_zip.open.return_value = mock_epub_zip_component

        mock_zip_package = mocker.patch("managers.s3.ZipFile")
        mock_zip_package.return_value.__enter__.return_value = mock_epub_zip

        mock_put = mocker.patch.object(S3Manager, "put_object")

        test_instance.store_epub(b"testObj", "10.10/testKey.epub", "testBucket")

        mock_put.assert_has_calls(
            [
                mocker.call(
                    object="compBytes1", key="10.10/testKey/comp1", bucket="testBucket"
                ),
                mocker.call(
                    object="compBytes2", key="10.10/testKey/comp2", bucket="testBucket"
                ),
            ]
        )

    def test_get_object_success(self, test_instance: S3Manager):
        test_instance.client.get_object.return_value = "testObject"

        test_response = test_instance.get_object("testKey", "testBucket")

        assert test_response == "testObject"
        test_instance.client.get_object.assert_called_once_with(
            Bucket="testBucket", Key="testKey"
        )

    def test_get_object_error(self, test_instance: S3Manager):
        test_instance.client.get_object.side_effect = ClientError({}, "Testing")

        with pytest.raises(S3Error):
            test_instance.get_object("testKey", "testBucket")

    def test_get_md5_hash(self):
        S3Manager.get_md5_hash(
            "testing".encode("utf-8")
        ) == "ae2b1fca515949e5d54fb22b8ed95575"
