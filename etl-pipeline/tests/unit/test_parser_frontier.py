import pytest
import requests
from requests.exceptions import ReadTimeout

from managers.parsers import FrontierParser


class TestFrontierParser:
    @pytest.fixture
    def testParser(self, mocker):
        mockRecord = mocker.MagicMock(
            title='testRecord',
            source='testSource',
            identifiers=['1|test', '2|other']
        )
        return FrontierParser('www.frontiersin.org/research-topics/1/1', 'testType', mockRecord)

    def test_initializer(self, testParser):
        assert testParser.uriIdentifier == None

    def test_validateURI_true(self, testParser):
        assert testParser.validateURI() is True
        assert testParser.uriIdentifier == '1'

    def test_validateURI_false(self, testParser):
        testParser.uri = 'otherURI'
        assert testParser.validateURI() is False

    def test_createLinks(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(FrontierParser,
            generateS3Root=mocker.DEFAULT,
            generatePDFLinks=mocker.DEFAULT,
            generateEpubLinks=mocker.DEFAULT
        )

        parserMocks['generateS3Root'].return_value = 'testRoot/'
        parserMocks['generatePDFLinks'].return_value = ['pdf1', None]
        parserMocks['generateEpubLinks'].return_value = ['epub1', 'epub2']

        testLinks = testParser.createLinks()

        assert testLinks == ['pdf1', 'epub1', 'epub2']
        parserMocks['generateS3Root'].assert_called_once()
        parserMocks['generatePDFLinks'].assert_called_once_with('testRoot/')
        parserMocks['generateEpubLinks'].assert_called_once_with('testRoot/')

    def test_generateEpubLinks_success(self, testParser, mocker):
        mockCheck = mocker.patch.object(FrontierParser, 'checkAvailability')
        mockCheck.return_value = (200, {'content-disposition': 'filename=title.EPUB'})

        testParser.uriIdentifier = 1
        testLinks = testParser.generateEpubLinks('testRoot/')

        assert testLinks == [
            ('testRoot/epubs/frontier/1_title/manifest.json', {'reader': True}, 'application/webpub+json', None, None),
            ('testRoot/epubs/frontier/1_title/META-INF/container.xml', {'reader': True}, 'application/epub+xml', None, None),
            ('testRoot/epubs/frontier/1_title.epub', {'download': True}, 'application/epub+zip', None, ('epubs/frontier/1_title.epub', 'https://www.frontiersin.org/research-topics/1/epub'))
        ]
        mockCheck.assert_called_once_with('https://www.frontiersin.org/research-topics/1/epub')

    def test_generateEpubLinks_request_error(self, testParser, mocker):
        mockCheck = mocker.patch.object(FrontierParser, 'checkAvailability')
        mockCheck.return_value = (500, {})

        testParser.uriIdentifier = 1
        testLinks = testParser.generateEpubLinks('testRoot/')

        assert testLinks == [None]

    def test_generateEpubLinks_header_error(self, testParser, mocker):
        mockResp = mocker.MagicMock(status_code=200, headers={'other': 'value'})
        mockHead = mocker.patch.object(requests, 'head')
        mockHead.return_value = mockResp

        testParser.uriIdentifier = 1
        testLinks = testParser.generateEpubLinks('testRoot/')

        assert testLinks == [None]

    def test_generatePDFLinks(self, testParser, mocker):
        mockGenerate = mocker.patch.object(FrontierParser, 'generateManifest')
        mockGenerate.return_value = 'testManifestJSON'

        testParser.uriIdentifier = 1
        testLinks = testParser.generatePDFLinks('testRoot/')

        assert testLinks == [
            ('testRoot/manifests/frontier/1.json', {'reader': True}, 'application/webpub+json', ('manifests/frontier/1.json', 'testManifestJSON'), None),
            ('https://www.frontiersin.org/research-topics/1/pdf', {'download': True}, 'application/pdf', None, None)
        ]

    def test_generateManifest(self, testParser, mocker):
        mockAbstractManifest = mocker.patch('managers.parsers.ParserABC.ParserABC.generateManifest')
        mockAbstractManifest.return_value = 'testManifest'

        assert testParser.generateManifest('sourceURI', 'manifestURI') == 'testManifest'
        mockAbstractManifest.assert_called_once_with('sourceURI', 'manifestURI')

    def test_generateS3Root(self, testParser, mocker):
        mockAbstractGenerate = mocker.patch('managers.parsers.ParserABC.ParserABC.generateS3Root')
        mockAbstractGenerate.return_value = 'testRoot'

        assert testParser.generateS3Root() == 'testRoot'
        mockAbstractGenerate.assert_called_once()

    def test_checkAvailability(self, mocker):
        mockResp = mocker.MagicMock(status_code=200, headers='testHeaders')
        mockHead = mocker.patch.object(requests, 'head')
        mockHead.return_value = mockResp

        assert FrontierParser.checkAvailability('testURL') == (200, 'testHeaders')

    def test_checkAvailability_timeout(self, mocker):
        mockHead = mocker.patch.object(requests, 'head')
        mockHead.side_effect = ReadTimeout

        assert FrontierParser.checkAvailability('testURL') == (0, None)
