import pytest
import requests
from requests.exceptions import HTTPError

from managers.cover_fetchers import HathiFetcher
from managers.cover_fetchers.hathi_fetcher import HathiCoverError, HathiPage


class TestHathiFetcher:
    @pytest.fixture
    def test_fetcher(self):
        class MockHathiFetcher(HathiFetcher):
            def __init__(self, *args):
                self.api_root = 'test_api_root'
                self.api_key = 'test_api_key'
                self.api_secret = 'test_api_secret'

        return MockHathiFetcher()

    @pytest.fixture
    def mock_mets_object(self):
        return {
            'METS:structMap': {
                'METS:div': {
                    'METS:div': [i for i in range(25)]
                }
            }
        }

    @pytest.fixture
    def mock_pages(self, mocker):
        return [mocker.MagicMock(page_score=i * 0.75, page_number=i) for i in range(25)]

    def test_has_cover_true(self, test_fetcher, mocker):
        mocker.patch.object(HathiFetcher, 'fetch_volume_cover')

        test_fetcher.identifiers = [('1', 'test'), ('tst.2', 'hathi')]
        assert test_fetcher.has_cover() == True

    def test_has_cover_false(self, test_fetcher, mocker):
        mocker.patch.object(HathiFetcher, 'fetch_volume_cover')

        test_fetcher.identifiers = [('1', 'test'), ('2', 'hathi')]
        assert test_fetcher.has_cover() == False

    def test_has_cover_fetch_error(self, test_fetcher, mocker):
        mock_fetch = mocker.patch.object(HathiFetcher, 'fetch_volume_cover')
        mock_fetch.side_effect = HathiCoverError('test error')

        test_fetcher.identifiers = [('1', 'test'), ('tst.2', 'hathi')]
        assert test_fetcher.has_cover() == False


    def test_fetch_volume_cover_success(self, test_fetcher, mock_mets_object, mock_pages, mocker):
        mock_response = mocker.MagicMock()
        mock_response.json.return_value = mock_mets_object
        mock_request = mocker.patch.object(HathiFetcher, 'make_hathi_req')
        mock_request.return_value = mock_response

        mock_page = mocker.patch('managers.cover_fetchers.hathi_fetcher.HathiPage')
        mock_page.side_effect = mock_pages

        mock_set_cover_page_url = mocker.patch.object(HathiFetcher, 'set_cover_page_url')

        test_fetcher.fetch_volume_cover(1)

        mock_set_cover_page_url.assert_called_once_with(1, 24)
        mock_request.assert_called_once_with('test_api_root/structure/1?format=json&v=2')

    def test_fetch_volume_cover_mets_error(self, test_fetcher, mock_mets_object, mocker):
        mock_response = mocker.MagicMock()
        mock_mets_object['METS:structMap']['METS:div']['METS:div'] = {i: 'data' for i in range(25)}
        mock_response.json.return_value = mock_mets_object
        mock_request = mocker.patch.object(HathiFetcher, 'make_hathi_req')
        mock_request.return_value = mock_response

        with pytest.raises(HathiCoverError):
            test_fetcher.fetch_volume_cover(1)

    def test_fetch_volume_cover_error(self, test_fetcher, mocker):
        mock_request = mocker.patch.object(HathiFetcher, 'make_hathi_req')
        mock_request.return_value = None

        with pytest.raises(HathiCoverError):
            test_fetcher.fetch_volume_cover(1)

    def test_set_cover_page_url(self, test_fetcher):
        test_fetcher.set_cover_page_url(1, 10)
        assert test_fetcher.uri == 'test_api_root/volume/pageimage/1/10?format=jpeg&v=2'
        assert test_fetcher.media_type == 'image/jpeg'

    def test_download_cover_file_success(self, test_fetcher, mocker):
        mock_response = mocker.MagicMock(content='test_cover_content')
        mock_request = mocker.patch.object(HathiFetcher, 'make_hathi_req')
        mock_request.return_value = mock_response

        test_fetcher.uri = 'test_uri'
        assert test_fetcher.download_cover_file() == 'test_cover_content'

    def test_download_cover_file_error(self, test_fetcher, mocker):
        mock_request = mocker.patch.object(HathiFetcher, 'make_hathi_req')
        mock_request.return_value = None

        test_fetcher.uri = 'test_uri'
        assert test_fetcher.download_cover_file() == None

    def test_generate_auth(self, test_fetcher, mocker):
        mock_auth = mocker.patch('managers.cover_fetchers.hathi_fetcher.OAuth1')
        mock_auth.return_value = 'test_auth_object'

        assert test_fetcher.generate_auth() == 'test_auth_object'
        mock_auth.assert_called_once_with('test_api_key', client_secret='test_api_secret', signature_type='query')

    def test_make_hathi_req_success(self, test_fetcher, mocker):
        mock_response = mocker.MagicMock()
        mock_get = mocker.patch.object(requests, 'get')
        mock_get.return_value = mock_response

        mock_auth = mocker.patch.object(HathiFetcher, 'generate_auth')
        mock_auth.return_value = 'test_auth_object'

        assert test_fetcher.make_hathi_req('test_uri') == mock_response
        mock_get.assert_called_once_with('test_uri', auth='test_auth_object', timeout=5)

    def test_get_api_response_error(self, test_fetcher, mocker):
        mock_response = mocker.MagicMock()
        mock_response.raise_for_status.side_effect = HTTPError
        mock_get = mocker.patch.object(requests, 'get')
        mock_get.return_value = mock_response

        mock_auth = mocker.patch.object(HathiFetcher, 'generate_auth')
        mock_auth.return_value = 'test_auth_object'

        assert test_fetcher.make_hathi_req('test_uri') == None

class TestHathiPage:
    @pytest.fixture
    def test_page(self, mocker):
        class MockHathiPage(HathiPage):
            def __init__(self, data):
                self.page_data = data
        
        return MockHathiPage({})
    
    def test_initializer(self, mocker):
        mocker.patch.multiple(HathiPage,
            get_page_number=mocker.DEFAULT,
            get_page_flags=mocker.DEFAULT,
            get_page_score=mocker.DEFAULT
        )

        test_page = HathiPage({})

        assert test_page.page_data == {}
        assert isinstance(test_page.page_number, mocker.MagicMock)
        assert isinstance(test_page.page_flags, mocker.MagicMock)
        assert isinstance(test_page.page_score, mocker.MagicMock)

    def test_get_page_number_present(self, test_page):
        test_page.page_data = {'ORDER': 10}

        assert test_page.get_page_number() == 10

    def test_get_page_number_missing(self, test_page):
        assert test_page.get_page_number() == 0

    def test_get_page_flags_present(self, test_page):
        test_page.page_data = {'LABEL': 'TESTING, OTHER'}

        assert test_page.get_page_flags() == set(['TESTING', 'OTHER'])

    def test_get_page_flags_missing(self, test_page):
        assert test_page.get_page_flags() == set([''])

    def test_get_page_score_present(self, test_page):
        test_page.page_flags = set(['FRONT_COVER', 'IMAGE_ON_PAGE'])

        assert test_page.get_page_score() == 2

    def test_get_page_score_missing(self, test_page):
        test_page.page_flags = set([])

        assert test_page.get_page_score() == 0
