import pytest
import requests
from requests.exceptions import HTTPError

from managers.cover_fetchers import GoogleBooksFetcher


class TestGoogleBooksFetcher:
    @pytest.fixture
    def testFetcher(self):
        class MockGBFetcher(GoogleBooksFetcher):
            def __init__(self, *args):
                self.apiKey = 'testAPIKey'

        return MockGBFetcher()

    def test_hasCover_true(self, testFetcher, mocker):
        mockFetch = mocker.patch.object(GoogleBooksFetcher, 'fetchVolume')
        mockFetch.return_value = True

        mockCover = mocker.patch.object(GoogleBooksFetcher, 'fetchCover')
        mockCover.return_value = True

        testFetcher.identifiers = [(1, 'test'), (2, 'isbn')]
        assert testFetcher.hasCover() == True

    def test_hasCover_false(self, testFetcher, mocker):
        mockFetch = mocker.patch.object(GoogleBooksFetcher, 'fetchVolume')
        mockFetch.return_value = False

        testFetcher.identifiers = [(1, 'test'), (2, 'isbn')]
        assert testFetcher.hasCover() == False

    def test_fetchVolume_success(self, testFetcher, mocker):
        mockGetResponse = mocker.patch.object(GoogleBooksFetcher, 'getAPIResponse')
        mockGetResponse.return_value = {'kind': 'books#volumes', 'totalItems': 1, 'items': ['testItem']}

        assert testFetcher.fetchVolume(1, 'isbn') == 'testItem'
        mockGetResponse.assert_called_once_with('https://www.googleapis.com/books/v1/volumes?q=isbn:1&key=testAPIKey')

    def test_fetchVolume_missing(self, testFetcher, mocker):
        mockGetResponse = mocker.patch.object(GoogleBooksFetcher, 'getAPIResponse')
        mockGetResponse.return_value = {'kind': 'books#volumes', 'totalItems': 0, 'items': []}

        assert testFetcher.fetchVolume(1, 'isbn') == None
        mockGetResponse.assert_called_once_with('https://www.googleapis.com/books/v1/volumes?q=isbn:1&key=testAPIKey')

    def test_fetchVolume_array_missing(self, testFetcher, mocker):
        mockGetResponse = mocker.patch.object(GoogleBooksFetcher, 'getAPIResponse')
        mockGetResponse.return_value = {'kind': 'books#volumes', 'totalItems': 0}

        assert testFetcher.fetchVolume(1, 'isbn') == None
        mockGetResponse.assert_called_once_with('https://www.googleapis.com/books/v1/volumes?q=isbn:1&key=testAPIKey')

    def test_fetchCover_success(self, testFetcher, mocker):
        mockGetResponse = mocker.patch.object(GoogleBooksFetcher, 'getAPIResponse')
        mockGetResponse.return_value = {'volumeInfo': {'imageLinks': {'large': 'largeLink', 'small': 'smallLink'}}}

        assert testFetcher.fetchCover({'id': 1}) == True
        assert testFetcher.uri == 'smallLink'
        assert testFetcher.mediaType == 'image/jpeg'
        mockGetResponse.assert_called_once_with('https://www.googleapis.com/books/v1/volumes/1?key=testAPIKey')

    def test_fetchCover_error(self, testFetcher, mocker):
        mockGetResponse = mocker.patch.object(GoogleBooksFetcher, 'getAPIResponse')
        mockGetResponse.return_value = {'volumeInfo': {}}

        assert testFetcher.fetchCover({'id': 1}) == False
        mockGetResponse.assert_called_once_with('https://www.googleapis.com/books/v1/volumes/1?key=testAPIKey')

    def test_fetchCover_no_matching_size(self, testFetcher, mocker):
        mockGetResponse = mocker.patch.object(GoogleBooksFetcher, 'getAPIResponse')
        mockGetResponse.return_value = {'volumeInfo': {'imageLinks': {'large': 'largeLink'}}}

        assert testFetcher.fetchCover({'id': 1}) == False
        mockGetResponse.assert_called_once_with('https://www.googleapis.com/books/v1/volumes/1?key=testAPIKey')

    def test_downloadCoverFile_success(self, testFetcher, mocker):
        mockResponse = mocker.MagicMock(content='testImageContent')
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResponse

        testFetcher.uri = 'testURI'
        assert testFetcher.downloadCoverFile() == 'testImageContent'

    def test_downloadCoverFile_error(self, testFetcher, mocker):
        mockResponse = mocker.MagicMock(content='testImageContent')
        mockResponse.raise_for_status.side_effect = HTTPError
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResponse

        testFetcher.uri = 'testURI'
        assert testFetcher.downloadCoverFile() == None

    def test_getAPIResponse_success(self, mocker):
        mockResponse = mocker.MagicMock()
        mockResponse.json.return_value = 'testJSON'
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResponse

        assert GoogleBooksFetcher.getAPIResponse('testURI') == 'testJSON'

    def test_getAPIResponse_error(self, mocker):
        mockResponse = mocker.MagicMock()
        mockResponse.raise_for_status.side_effect = HTTPError
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResponse

        assert GoogleBooksFetcher.getAPIResponse('testURI') == None
