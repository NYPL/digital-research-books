import pytest
import requests

from managers.parsers import DeGruyterParser


class TestDeGruyterParser:
    @pytest.fixture
    def testParser(self, mocker):
        mockRecord = mocker.MagicMock(
            title='testRecord',
            source='testSource',
            identifiers=['1|test', '2|other']
        )
        return DeGruyterParser('www.degruyter.com/book/1', 'testType', mockRecord)

    def test_initializer(self, testParser):
        assert testParser.uriIdentifier == None

    def test_validateURI_true(self, testParser):
        assert testParser.validateURI() is True

    def test_validateURI_false(self, testParser):
        testParser.uri = 'otherURI'
        assert testParser.validateURI() is False

    def test_createLinks_isbn_match(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(DeGruyterParser,
            generateS3Root=mocker.DEFAULT,
            generatePDFLinks=mocker.DEFAULT,
            generateEpubLinks=mocker.DEFAULT
        )

        parserMocks['generateS3Root'].return_value = 'testRoot/'
        parserMocks['generatePDFLinks'].return_value = ['pdf1', None]
        parserMocks['generateEpubLinks'].return_value = ['epub1', 'epub2']

        testParser.uri = 'degruyter.com/document/doi/10.000/9781234567890/html'

        testLinks = testParser.createLinks()

        assert testLinks == ['pdf1', 'epub1', 'epub2']
        parserMocks['generateS3Root'].assert_called_once()
        parserMocks['generatePDFLinks'].assert_called_once_with(
            'testRoot/', 'https://www.degruyter.com/document/doi/10.000/9781234567890/pdf'
        )
        parserMocks['generateEpubLinks'].assert_called_once_with(
            'testRoot/', 'https://www.degruyter.com/document/doi/10.000/9781234567890/epub'
        )

    def test_createLinks_title_match(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(DeGruyterParser,
            generateS3Root=mocker.DEFAULT,
            generatePDFLinks=mocker.DEFAULT,
            generateEpubLinks=mocker.DEFAULT
        )

        parserMocks['generateS3Root'].return_value = 'testRoot/'
        parserMocks['generatePDFLinks'].return_value = ['pdf1', None]
        parserMocks['generateEpubLinks'].return_value = ['epub1', 'epub2']

        testParser.uri = 'degruyter.com/title/1'

        testLinks = testParser.createLinks()

        assert testLinks == ['pdf1', 'epub1', 'epub2']
        parserMocks['generateS3Root'].assert_called_once()
        parserMocks['generatePDFLinks'].assert_called_once_with(
            'testRoot/', 'https://www.degruyter.com/downloadpdf/title/1'
        )
        parserMocks['generateEpubLinks'].assert_called_once_with(
            'testRoot/', 'https://www.degruyter.com/downloadepub/title/1'
        )

    def test_createLinks_no_match(self, testParser, mocker):
        mockGenerate = mocker.patch.object(DeGruyterParser, 'generateS3Root')
        mockGenerate.return_value = 'testRoot/'

        mockAbstractCreate = mocker.patch('managers.parsers.ParserABC.ParserABC.createLinks')
        mockAbstractCreate.return_value = ['testLink']

        testLinks = testParser.createLinks()

        assert testLinks == ['testLink']
        mockGenerate.assert_called_once()
        mockAbstractCreate.assert_called_once()

    def test_generateEpubLinks_success(self, testParser, mocker):
        mockHead = mocker.patch.object(DeGruyterParser, 'makeHeadQuery')
        mockHead.return_value = (200, 'mockHeader')

        testParser.uriIdentifier = 1
        testLinks = testParser.generateEpubLinks('testRoot/', 'epubSourceURI')

        assert testLinks == [
            ('testRoot/epubs/degruyter/1/manifest.json', {'reader': True}, 'application/webpub+json', None, None),
            ('testRoot/epubs/degruyter/1/META-INF/container.xml', {'reader': True}, 'application/epub+xml', None, None),
            ('testRoot/epubs/degruyter/1.epub', {'download': True}, 'application/epub+zip', None, ('epubs/degruyter/1.epub', 'epubSourceURI'))
        ]
        mockHead.assert_called_once_with('epubSourceURI')

    def test_generateEpubLinks_error(self, testParser, mocker):
        mockHead = mocker.patch.object(DeGruyterParser, 'makeHeadQuery')
        mockHead.return_value = (500, 'mockHeader')

        testParser.uriIdentifier = 1
        testLinks = testParser.generateEpubLinks('testRoot/', 'epubSourceURI')

        assert testLinks == [None]
        mockHead.assert_called_once_with('epubSourceURI')

    def test_generatePDFLinks(self, testParser, mocker):
        mockGenerate = mocker.patch.object(DeGruyterParser, 'generateManifest')
        mockGenerate.return_value = 'testManifestJSON'

        testParser.uriIdentifier = 1
        testLinks = testParser.generatePDFLinks('testRoot/', 'pdfSourceURI')

        assert testLinks == [
            ('testRoot/manifests/degruyter/1.json', {'reader': True}, 'application/webpub+json', ('manifests/degruyter/1.json', 'testManifestJSON'), None),
            ('pdfSourceURI', {'download': True}, 'application/pdf', None, None)
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

    def test_makeHeadQuery(self, mocker):
        mockResponse = mocker.MagicMock()
        mockResponse.status_code = 200
        mockResponse.headers = 'testHeaders'

        mockHead = mocker.patch.object(requests, 'head')
        mockHead.return_value = mockResponse

        assert DeGruyterParser.makeHeadQuery('testURI') == (200, 'testHeaders')
        mockHead.assert_called_once_with(
            'testURI', timeout=15, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)'}
        )
