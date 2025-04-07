import pytest
import requests
from requests.exceptions import HTTPError

from managers.cover_fetchers import HathiFetcher
from managers.cover_fetchers.hathiFetcher import HathiCoverError


class TestHathiFetcher:
    @pytest.fixture
    def testFetcher(self):
        class MockHathiFetcher(HathiFetcher):
            def __init__(self, *args):
                self.apiRoot = 'testAPIRoot'
                self.apiKey = 'testAPIKey'
                self.apiSecret = 'testAPISecret'

        return MockHathiFetcher()

    @pytest.fixture
    def mockMETSObject(self):
        return {
            'METS:structMap': {
                'METS:div': {
                    'METS:div': [i for i in range(25)]
                }
            }
        }

    @pytest.fixture
    def mockPages(self, mocker):
        return [mocker.MagicMock(pageScore=i * 0.75, pageNumber=i) for i in range(25)]

    def test_hasCover_true(self, testFetcher, mocker):
        mocker.patch.object(HathiFetcher, 'fetchVolumeCover')

        testFetcher.identifiers = [('1', 'test'), ('tst.2', 'hathi')]
        assert testFetcher.hasCover() == True

    def test_hasCover_false(self, testFetcher, mocker):
        mocker.patch.object(HathiFetcher, 'fetchVolumeCover')

        testFetcher.identifiers = [('1', 'test'), ('2', 'hathi')]
        assert testFetcher.hasCover() == False

    def test_hasCover_fetch_error(self, testFetcher, mocker):
        mockFetch = mocker.patch.object(HathiFetcher, 'fetchVolumeCover')
        mockFetch.side_effect = HathiCoverError('test error')

        testFetcher.identifiers = [('1', 'test'), ('tst.2', 'hathi')]
        assert testFetcher.hasCover() == False


    def test_fetchVolumeCover_success(self, testFetcher, mockMETSObject, mockPages, mocker):
        mockResponse = mocker.MagicMock()
        mockResponse.json.return_value = mockMETSObject
        mockRequest = mocker.patch.object(HathiFetcher, 'makeHathiReq')
        mockRequest.return_value = mockResponse

        mockPage = mocker.patch('managers.cover_fetchers.hathiFetcher.HathiPage')
        mockPage.side_effect = mockPages

        mockSetCover = mocker.patch.object(HathiFetcher, 'setCoverPageURL')

        testFetcher.fetchVolumeCover(1)

        mockSetCover.assert_called_once_with(1, 24)
        mockRequest.assert_called_once_with('testAPIRoot/structure/1?format=json&v=2')

    def test_fetchVolumeCover_mets_error(self, testFetcher, mockMETSObject, mocker):
        mockResponse = mocker.MagicMock()
        mockMETSObject['METS:structMap']['METS:div']['METS:div'] = {i: 'data' for i in range(25)}
        mockResponse.json.return_value = mockMETSObject
        mockRequest = mocker.patch.object(HathiFetcher, 'makeHathiReq')
        mockRequest.return_value = mockResponse

        with pytest.raises(HathiCoverError):
            testFetcher.fetchVolumeCover(1)

    def test_fetchVolumeCover_error(self, testFetcher, mocker):
        mockRequest = mocker.patch.object(HathiFetcher, 'makeHathiReq')
        mockRequest.return_value = None

        with pytest.raises(HathiCoverError):
            testFetcher.fetchVolumeCover(1)

    def test_setCoverPageURL(self, testFetcher):
        testFetcher.setCoverPageURL(1, 10)
        assert testFetcher.uri == 'testAPIRoot/volume/pageimage/1/10?format=jpeg&v=2'
        assert testFetcher.mediaType == 'image/jpeg'

    def test_downloadCoverFile_success(self, testFetcher, mocker):
        mockResponse = mocker.MagicMock(content='testCoverContent')
        mockRequest = mocker.patch.object(HathiFetcher, 'makeHathiReq')
        mockRequest.return_value = mockResponse

        testFetcher.uri = 'testURI'
        assert testFetcher.downloadCoverFile() == 'testCoverContent'

    def test_downloadCoverFile_error(self, testFetcher, mocker):
        mockRequest = mocker.patch.object(HathiFetcher, 'makeHathiReq')
        mockRequest.return_value = None

        testFetcher.uri = 'testURI'
        assert testFetcher.downloadCoverFile() == None

    def test_generateAuth(self, testFetcher, mocker):
        mockAuth = mocker.patch('managers.cover_fetchers.hathiFetcher.OAuth1')
        mockAuth.return_value = 'testAuthObject'

        assert testFetcher.generateAuth() == 'testAuthObject'
        mockAuth.assert_called_once_with('testAPIKey', client_secret='testAPISecret', signature_type='query')

    def test_makeHathiReq_success(self, testFetcher, mocker):
        mockResponse = mocker.MagicMock()
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResponse

        mockAuth = mocker.patch.object(HathiFetcher, 'generateAuth')
        mockAuth.return_value = 'testAuthObject'

        assert testFetcher.makeHathiReq('testURI') == mockResponse
        mockGet.assert_called_once_with('testURI', auth='testAuthObject', timeout=5)

    def test_getAPIResponse_error(self, testFetcher, mocker):
        mockResponse = mocker.MagicMock()
        mockResponse.raise_for_status.side_effect = HTTPError
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResponse

        mockAuth = mocker.patch.object(HathiFetcher, 'generateAuth')
        mockAuth.return_value = 'testAuthObject'

        assert testFetcher.makeHathiReq('testURI') == None