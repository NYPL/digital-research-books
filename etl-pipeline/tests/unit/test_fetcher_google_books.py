import pytest
import requests
from requests.exceptions import HTTPError

from managers.cover_fetchers import GoogleBooksFetcher


class TestGoogleBooksFetcher:
    @pytest.fixture
    def test_fetcher(self):
        class MockGBFetcher(GoogleBooksFetcher):
            def __init__(self, *args):
                self.api_key = 'test_api_key'

        return MockGBFetcher()

    def test_has_cover_true(self, test_fetcher, mocker):
        mock_fetch = mocker.patch.object(GoogleBooksFetcher, 'fetch_volume')
        mock_fetch.return_value = True

        mock_fetch_cover = mocker.patch.object(GoogleBooksFetcher, 'fetch_cover')
        mock_fetch_cover.return_value = True

        test_fetcher.identifiers = [(1, 'test'), (2, 'isbn')]
        assert test_fetcher.has_cover() == True

    def test_has_cover_false(self, test_fetcher, mocker):
        mock_fetch = mocker.patch.object(GoogleBooksFetcher, 'fetch_volume')
        mock_fetch.return_value = False

        test_fetcher.identifiers = [(1, 'test'), (2, 'isbn')]
        assert test_fetcher.has_cover() == False

    def test_fetch_volume_success(self, test_fetcher, mocker):
        mock_get_api_response = mocker.patch.object(GoogleBooksFetcher, 'get_api_response')
        mock_get_api_response.return_value = {'kind': 'books#volumes', 'totalItems': 1, 'items': ['testItem']}

        assert test_fetcher.fetch_volume(1, 'isbn') == 'testItem'
        mock_get_api_response.assert_called_once_with('https://www.googleapis.com/books/v1/volumes?q=isbn:1&key=test_api_key')

    def test_fetch_volume_missing(self, test_fetcher, mocker):
        mock_get_api_response = mocker.patch.object(GoogleBooksFetcher, 'get_api_response')
        mock_get_api_response.return_value = {'kind': 'books#volumes', 'totalItems': 0, 'items': []}

        assert test_fetcher.fetch_volume(1, 'isbn') == None
        mock_get_api_response.assert_called_once_with('https://www.googleapis.com/books/v1/volumes?q=isbn:1&key=test_api_key')

    def test_fetch_volume_array_missing(self, test_fetcher, mocker):
        mock_get_api_response = mocker.patch.object(GoogleBooksFetcher, 'get_api_response')
        mock_get_api_response.return_value = {'kind': 'books#volumes', 'totalItems': 0}

        assert test_fetcher.fetch_volume(1, 'isbn') == None
        mock_get_api_response.assert_called_once_with('https://www.googleapis.com/books/v1/volumes?q=isbn:1&key=test_api_key')

    def test_fetch_cover_success(self, test_fetcher, mocker):
        mock_get_api_response = mocker.patch.object(GoogleBooksFetcher, 'get_api_response')
        mock_get_api_response.return_value = {'volumeInfo': {'imageLinks': {'large': 'largeLink', 'small': 'smallLink'}}}

        assert test_fetcher.fetch_cover({'id': 1}) == True
        assert test_fetcher.uri == 'smallLink'
        assert test_fetcher.media_type == 'image/jpeg'
        mock_get_api_response.assert_called_once_with('https://www.googleapis.com/books/v1/volumes/1?key=test_api_key')

    def test_fetch_cover_error(self, test_fetcher, mocker):
        mock_get_api_response = mocker.patch.object(GoogleBooksFetcher, 'get_api_response')
        mock_get_api_response.return_value = {'volumeInfo': {}}

        assert test_fetcher.fetch_cover({'id': 1}) == False
        mock_get_api_response.assert_called_once_with('https://www.googleapis.com/books/v1/volumes/1?key=test_api_key')

    def test_fetch_cover_no_matching_size(self, test_fetcher, mocker):
        mock_get_api_response = mocker.patch.object(GoogleBooksFetcher, 'get_api_response')
        mock_get_api_response.return_value = {'volumeInfo': {'imageLinks': {'large': 'largeLink'}}}

        assert test_fetcher.fetch_cover({'id': 1}) == False
        mock_get_api_response.assert_called_once_with('https://www.googleapis.com/books/v1/volumes/1?key=test_api_key')

    def test_download_cover_file_success(self, test_fetcher, mocker):
        mock_response = mocker.MagicMock(content='testImageContent')
        mock_get = mocker.patch.object(requests, 'get')
        mock_get.return_value = mock_response

        test_fetcher.uri = 'testURI'
        assert test_fetcher.download_cover_file() == 'testImageContent'

    def test_download_cover_file_error(self, test_fetcher, mocker):
        mock_response = mocker.MagicMock(content='testImageContent')
        mock_response.raise_for_status.side_effect = HTTPError
        mock_get = mocker.patch.object(requests, 'get')
        mock_get.return_value = mock_response

        test_fetcher.uri = 'testURI'
        assert test_fetcher.download_cover_file() == None

    def test_get_api_response_success(self, mocker):
        mock_response = mocker.MagicMock()
        mock_response.json.return_value = 'testJSON'
        mock_get = mocker.patch.object(requests, 'get')
        mock_get.return_value = mock_response

        assert GoogleBooksFetcher.get_api_response('testURI') == 'testJSON'

    def test_get_api_response_error(self, mocker):
        mock_response = mocker.MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError
        mock_get = mocker.patch.object(requests, 'get')
        mock_get.return_value = mock_response

        assert GoogleBooksFetcher.get_api_response('testURI') == None
