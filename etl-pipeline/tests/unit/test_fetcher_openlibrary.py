import pytest
import requests
from requests.exceptions import HTTPError

from managers.cover_fetchers import OpenLibraryFetcher
from model import OpenLibraryCover


class TestOpenLibraryFetcher:
    @pytest.fixture
    def testFetcher(self, mocker):
        class MockOLFetcher(OpenLibraryFetcher):
            def __init__(self, *args):
                self.session = mocker.MagicMock()

        return MockOLFetcher()

    def test_hasCover_true(self, testFetcher, mocker):
        mockVolumeCover = mocker.patch.object(OpenLibraryFetcher, 'fetchVolumeCover')
        mockVolumeCover.return_value = True

        testFetcher.identifiers = [(1, 'test'), (2, 'lccn')]
        assert testFetcher.hasCover() == True
        assert testFetcher.coverID == 'lccn_2'

    def test_hasCover_false(self, testFetcher, mocker):
        mockVolumeCover = mocker.patch.object(OpenLibraryFetcher, 'fetchVolumeCover')
        mockVolumeCover.return_value = False

        testFetcher.identifiers = [(1, 'test'), (2, 'hathi')]
        assert testFetcher.hasCover() == False

    def test_fetchVolumeCover_success(self, testFetcher, mocker):
        mockSetCover = mocker.patch.object(OpenLibraryFetcher, 'setCoverPageURL')

        mockRow = mocker.MagicMock(cover_id=1)
        testFetcher.session.query().filter().filter().first.return_value = mockRow

        assert testFetcher.fetchVolumeCover(1, 'test') == True
        mockSetCover.assert_called_once_with(1)
        testFetcher.session.query.call_args[0][0] == OpenLibraryCover
        assert testFetcher.session.query().filter.call_args[0][0].compare((OpenLibraryCover.name == 'test'))
        assert testFetcher.session.query().filter().filter.call_args[0][0].compare((OpenLibraryCover.value == 1))

    def test_fetchVolumeCover_failure(self, testFetcher, mocker):
        mockSetCover = mocker.patch.object(OpenLibraryFetcher, 'setCoverPageURL')

        testFetcher.session.query().filter().filter().first.return_value = None

        assert testFetcher.fetchVolumeCover(1, 'test') == False
        mockSetCover.assert_not_called()
        testFetcher.session.query.call_args[0][0] == OpenLibraryCover
        assert testFetcher.session.query().filter.call_args[0][0].compare((OpenLibraryCover.name == 'test'))
        assert testFetcher.session.query().filter().filter.call_args[0][0].compare((OpenLibraryCover.value == 1))

    def test_setCoverPageURL(self, testFetcher):
        testFetcher.setCoverPageURL(1)
        
        assert testFetcher.uri == 'http://covers.openlibrary.org/b/id/1-L.jpg'
        assert testFetcher.mediaType == 'image/jpeg'

    def test_downloadCoverFile_success(self, testFetcher, mocker):
        mockResponse = mocker.MagicMock(content='testCoverContent')
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResponse

        testFetcher.uri = 'testURI'
        assert testFetcher.downloadCoverFile() == 'testCoverContent'
        mockGet.assert_called_once_with('testURI', timeout=5)

    def test_getAPIResponse_error(self, testFetcher, mocker):
        mockResponse = mocker.MagicMock()
        mockResponse.raise_for_status.side_effect = HTTPError
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResponse

        testFetcher.uri = 'testURI'
        assert testFetcher.downloadCoverFile() == None