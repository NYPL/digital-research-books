import pytest
import requests

from managers.parsers import OpenEditionParser


class TestOpenEditionParser:
    @pytest.fixture
    def testParser(self, mocker):
        mockRecord = mocker.MagicMock(
            title='testRecord',
            source='testSource',
            identifiers=['1|test', '2|other']
        )
        return OpenEditionParser('books.openedition.org/p123/1', 'testType', mockRecord)

    @pytest.fixture
    def testOEPage(self):
        return open('./tests/fixtures/openeditions_book_14472.html', 'r').read()

    def test_initializer(self, testParser):
        assert testParser.uriIdentifier == None
        assert testParser.publisher == None

    def test_validateURI_true(self, testParser):
        assert testParser.validateURI() is True
        assert testParser.uriIdentifier == '1'
        assert testParser.publisher == 'p123'

    def test_validateURI_true_split(self, testParser):
        testParser.uri = 'Leader text: books.openedition.org/p123/1'

        assert testParser.validateURI() is True
        assert testParser.uri == 'http://books.openedition.org/p123/1'
        assert testParser.uriIdentifier == '1'
        assert testParser.publisher == 'p123'

    def test_validateURI_false(self, testParser):
        testParser.uri = 'otherURI'
        assert testParser.validateURI() is False

    def test_createLinks_epub(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(OpenEditionParser,
            loadEbookLinks=mocker.DEFAULT,
            createEpubLink=mocker.DEFAULT
        )
        parserMocks['loadEbookLinks'].return_value = [('epubURI', 'application/epub+xml', 'testFlags')]
        parserMocks['createEpubLink'].return_value = ['epubXML', 'epubZIP']

        testLinks = testParser.createLinks()

        assert testLinks == ['epubXML', 'epubZIP']
        parserMocks['loadEbookLinks'].assert_called_once()
        parserMocks['createEpubLink'].assert_called_once_with('epubURI', 'application/epub+xml', 'testFlags')

    def test_createLinks_pdf(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(OpenEditionParser,
            loadEbookLinks=mocker.DEFAULT,
            createPDFLink=mocker.DEFAULT
        )
        parserMocks['loadEbookLinks'].return_value = [('pdfURI', 'application/webpub+json', 'testFlags')]
        parserMocks['createPDFLink'].return_value = ['pdfJSON', 'pdfSource']

        testLinks = testParser.createLinks()

        assert testLinks == ['pdfJSON', 'pdfSource']
        parserMocks['loadEbookLinks'].assert_called_once()
        parserMocks['createPDFLink'].assert_called_once_with('pdfURI', 'application/webpub+json', 'testFlags')

    def test_createLinks_other(self, testParser, mocker):
        mockLoad = mocker.patch.object(OpenEditionParser, 'loadEbookLinks')
        mockLoad.return_value = [('otherURI', 'testFlags', 'type/test')]

        testLinks = testParser.createLinks()

        assert testLinks == [('otherURI', 'type/test', 'testFlags', None, None)]
        mockLoad.assert_called_once()

    def test_createEpubLink(self, testParser, mocker):
        mockRoot = mocker.patch.object(OpenEditionParser, 'generateS3Root')
        mockRoot.return_value = 'testRoot/'

        testParser.publisher = 'pub'
        testParser.uriIdentifier = 1
        testLinks = testParser.createEpubLink('sourceURI', 'application/epub+xml', {'reader': True})

        assert testLinks == [
            ('testRoot/epubs/doab/pub_1/manifest.json', {'reader': True}, 'application/webpub+json', None, None),
            ('testRoot/epubs/doab/pub_1/META-INF/container.xml', {'reader': True}, 'application/epub+xml', None, None),
            ('testRoot/epubs/doab/pub_1.epub', {'download': True}, 'application/epub+zip', None, ('epubs/doab/pub_1.epub', 'sourceURI'))
        ]

        mockRoot.assert_called_once()

    def test_createPDFLink(self, testParser, mocker):
        mockRoot = mocker.patch.object(OpenEditionParser, 'generateS3Root')
        mockRoot.return_value = 'testRoot/'

        mockGenerate = mocker.patch.object(OpenEditionParser, 'generateManifest')
        mockGenerate.return_value = 'testManifestJSON'

        testParser.publisher = 'pub'
        testParser.uriIdentifier = 1
        testLinks = testParser.createPDFLink('sourceURI', 'application/webpub+json', {'reader': True})

        assert testLinks == [
            ('testRoot/manifests/doab/pub_1.json', {'reader': True}, 'application/webpub+json', ('manifests/doab/pub_1.json', 'testManifestJSON'), None),
            ('sourceURI', {'download': True}, 'application/pdf', None, None)
        ]

    def test_loadEbookLinks_success(self, testParser, testOEPage, mocker):
        mockResp = mocker.MagicMock(status_code=200, text=testOEPage)
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResp

        mockParse = mocker.patch.object(OpenEditionParser, 'parseBookLink')
        mockParse.side_effect = ['openAccess', None, 'pdf', 'epub']

        testLinks = testParser.loadEbookLinks()

        assert testLinks == ['openAccess', 'pdf', 'epub']
        assert '14482' == mockParse.mock_calls[0].args[0].get('href')
        assert '14482?format=reader' == mockParse.mock_calls[1].args[0].get('href')
        assert 'http://books.openedition.org/cths/epub/14472' == mockParse.mock_calls[2].args[0].get('href')
        assert 'http://books.openedition.org/cths/pdf/14472' == mockParse.mock_calls[3].args[0].get('href')

    def test_loadEbookLinks_error(self, testParser, testOEPage, mocker):
        mockResp = mocker.MagicMock(status_code=500, text='errorPage')
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResp

        assert testParser.loadEbookLinks() == []
    
    def test_parseBookLink(self, testParser, mocker):
        mockLink = mocker.MagicMock()
        mockLink.get.return_value = '12345'

        testParser.publisher = 'test'
        testLink = testParser.parseBookLink(mockLink)

        assert testLink == (
            'http://books.openedition.org/test/12345', 'text/html', {}
        )
        mockLink.get.assert_called_once_with('href')

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
