import pytest
import requests
from requests.exceptions import HTTPError

from managers.cover_fetchers import OpenLibraryFetcher
from model import OpenLibraryCover


class TestOpenLibraryFetcher:
    @pytest.fixture
    def test_fetcher(self, mocker):
        class MockOLFetcher(OpenLibraryFetcher):
            def __init__(self, *args):
                self.session = mocker.MagicMock()

        return MockOLFetcher()

    def test_has_cover_true(self, test_fetcher, mocker):
        mock_volume_cover = mocker.patch.object(OpenLibraryFetcher, 'fetch_volume_cover')
        mock_volume_cover.return_value = True

        test_fetcher.identifiers = [(1, 'test'), (2, 'lccn')]
        assert test_fetcher.has_cover() == True
        assert test_fetcher.cover_id == 'lccn_2'

    def test_has_cover_false(self, test_fetcher, mocker):
        mock_volume_cover = mocker.patch.object(OpenLibraryFetcher, 'fetch_volume_cover')
        mock_volume_cover.return_value = False

        test_fetcher.identifiers = [(1, 'test'), (2, 'hathi')]
        assert test_fetcher.has_cover() == False

    def test_fetch_volume_cover_success(self, test_fetcher, mocker):
        mock_set_cover_page_url = mocker.patch.object(OpenLibraryFetcher, 'set_cover_page_url')

        mock_row = mocker.MagicMock(cover_id=1)
        test_fetcher.session.query().filter().filter().first.return_value = mock_row

        assert test_fetcher.fetch_volume_cover(1, 'test') == True
        mock_set_cover_page_url.assert_called_once_with(1)
        test_fetcher.session.query.call_args[0][0] == OpenLibraryCover
        assert test_fetcher.session.query().filter.call_args[0][0].compare((OpenLibraryCover.name == 'test'))
        assert test_fetcher.session.query().filter().filter.call_args[0][0].compare((OpenLibraryCover.value == 1))

    def test_fetch_volume_cover_failure(self, test_fetcher, mocker):
        mock_set_cover_page_url = mocker.patch.object(OpenLibraryFetcher, 'set_cover_page_url')

        test_fetcher.session.query().filter().filter().first.return_value = None

        assert test_fetcher.fetch_volume_cover(1, 'test') == False
        mock_set_cover_page_url.assert_not_called()
        test_fetcher.session.query.call_args[0][0] == OpenLibraryCover
        assert test_fetcher.session.query().filter.call_args[0][0].compare((OpenLibraryCover.name == 'test'))
        assert test_fetcher.session.query().filter().filter.call_args[0][0].compare((OpenLibraryCover.value == 1))

    def test_set_cover_page_url(self, test_fetcher):
        test_fetcher.set_cover_page_url(1)
        
        assert test_fetcher.uri == 'http://covers.openlibrary.org/b/id/1-L.jpg'
        assert test_fetcher.media_type == 'image/jpeg'

    def test_download_cover_file_success(self, test_fetcher, mocker):
        mock_response = mocker.MagicMock(content='test_cover_content')
        mock_get = mocker.patch.object(requests, 'get')
        mock_get.return_value = mock_response

        test_fetcher.uri = 'test_uri'
        assert test_fetcher.download_cover_file() == 'test_cover_content'
        mock_get.assert_called_once_with('test_uri', timeout=5)

    def test_download_cover_file_error(self, test_fetcher, mocker):
        mock_response = mocker.MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError
        mock_get = mocker.patch.object(requests, 'get')
        mock_get.return_value = mock_response

        test_fetcher.uri = 'test_uri'
        assert test_fetcher.download_cover_file() == None