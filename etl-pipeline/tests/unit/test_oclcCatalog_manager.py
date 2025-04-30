import pytest
from requests.exceptions import ConnectionError, Timeout

from managers import OCLCCatalogManager
from managers.oclc_catalog import OCLCCatalogError


class TestOCLCCatalogManager:
    @pytest.fixture
    def testInstance(self, mocker):
        return OCLCCatalogManager()

    def test_query_catalog_success(self, testInstance, mocker):
        mockResponse = mocker.MagicMock()
        mockRequest = mocker.patch("managers.oclc_catalog.requests")
        mockRequest.get.return_value = mockResponse

        mock_auth = mocker.patch(
            "managers.oclc_auth.OCLCAuthManager.get_metadata_token"
        )
        mock_auth.return_value = "foo"

        mockResponse.status_code = 200
        mockResponse.text = "testClassifyRecord"

        testResponse = testInstance.query_catalog(1)

        assert testResponse == "testClassifyRecord"
        mockRequest.get.assert_called_once_with(
            "https://metadata.api.oclc.org/worldcat/manage/bibs/1",
            headers={"Authorization": "Bearer foo"},
            timeout=5,
        )

    def test_query_catalog_error(self, testInstance, mocker):
        mockResponse = mocker.MagicMock()
        mockRequest = mocker.patch("managers.oclc_catalog.requests")
        mockRequest.get.return_value = mockResponse

        mock_auth = mocker.patch(
            "managers.oclc_auth.OCLCAuthManager.get_metadata_token"
        )
        mock_auth.return_value = "foo"

        mockResponse.status_code = 500
        mockResponse.text = "testClassifyRecord"

        testResponse = testInstance.query_catalog(1)

        assert testResponse == None
        mockRequest.get.assert_called_once_with(
            "https://metadata.api.oclc.org/worldcat/manage/bibs/1",
            headers={"Authorization": "Bearer foo"},
            timeout=5,
        )

    def test_query_catalog_single_retry_then_success(self, testInstance, mocker):
        mockResponse = mocker.MagicMock()
        mockRequest = mocker.patch("managers.oclc_catalog.requests")
        mockRequest.get.side_effect = [ConnectionError, mockResponse]

        mock_auth = mocker.patch(
            "managers.oclc_auth.OCLCAuthManager.get_metadata_token"
        )
        mock_auth.return_value = "foo"

        mockResponse.status_code = 200
        mockResponse.text = "testClassifyRecord"

        testResponse = testInstance.query_catalog(1)

        assert testResponse == "testClassifyRecord"
        mockRequest.get.assert_has_calls(
            [
                mocker.call(
                    "https://metadata.api.oclc.org/worldcat/manage/bibs/1",
                    timeout=5,
                    headers={"Authorization": "Bearer foo"},
                )
            ]
            * 2
        )

    def test_query_catalog_exhaust_retries(self, testInstance, mocker):
        mockResponse = mocker.MagicMock()
        mockRequest = mocker.patch("managers.oclc_catalog.requests")
        mockRequest.get.side_effect = [ConnectionError, ConnectionError, Timeout]

        mock_auth = mocker.patch(
            "managers.oclc_auth.OCLCAuthManager.get_metadata_token"
        )
        mock_auth.return_value = "foo"

        mockResponse.status_code = 200
        mockResponse.text = "testClassifyRecord"

        testResponse = testInstance.query_catalog(1)

        assert testResponse == None
        mockRequest.get.assert_has_calls(
            [
                mocker.call(
                    "https://metadata.api.oclc.org/worldcat/manage/bibs/1",
                    timeout=5,
                    headers={"Authorization": "Bearer foo"},
                )
            ]
            * 3
        )

    def test_generate_search_query_w_identifier(self, testInstance):
        assert (
            testInstance.generate_search_query(identifier_type="issn", identifier=1)
            == "in: 1"
        )

    def test_generate_search_query_wo_identifier(self, testInstance):
        assert (
            testInstance.generate_search_query(
                identifier=None, title="testTitle", author="testAuthor"
            )
            == "ti:testTitle au:testAuthor"
        )

    def test_generate_search_query_with_insufficient_data(self, testInstance):
        with pytest.raises(OCLCCatalogError):
            testInstance.generate_search_query(author="testAuthor")
