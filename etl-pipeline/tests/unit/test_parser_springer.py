import pytest
import requests

from managers.parsers import SpringerParser


class TestSpringerParser:
    @pytest.fixture
    def testParser(self, mocker):
        mockRecord = mocker.MagicMock(
            title='testRecord',
            source='testSource',
            identifiers=['1|test', '2|other']
        )
        return SpringerParser('link.springer.com/book/10.007/1', 'testType', mockRecord)

    @pytest.fixture
    def testSpringerPage(self):
        return open('./tests/fixtures/springer_book_9783642208973.html', 'r').read()

    def test_initializer(self, testParser):
        assert testParser.uriIdentifier == None
        assert testParser.code == None

    def test_validateURI_true(self, testParser):
        assert testParser.validateURI() is True
        assert testParser.uriIdentifier == '1'
        assert testParser.code == '10.007'

    def test_validateURI_alt_link(self, testParser, mocker):
        mockValidateAlt = mocker.patch.object(SpringerParser, 'validateAltLinkFormats')
        mockValidateAlt.return_value = True

        testParser.uri = 'springer.com/gp/book/978123456789'

        assert testParser.validateURI() is True
        assert testParser.uri == 'http://springer.com/gp/book/978123456789'

    def test_validateURI_false(self, testParser):
        testParser.uri = 'otherURI'
        assert testParser.validateURI() is False

    def test_validateAltLinkFormats_true(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(SpringerParser,
            findOALink=mocker.DEFAULT,
            validateURI=mocker.DEFAULT
        )
        parserMocks['validateURI'].return_value = True

        mockResp = mocker.MagicMock(headers={'Location': 'finalURI'})
        mockHead = mocker.patch.object(requests, 'head')
        mockHead.return_value = mockResp

        assert testParser.validateAltLinkFormats() is True
        assert testParser.uri == 'http://finalURI'
        mockHead.assert_called_once_with('http://link.springer.com/book/10.007/1', timeout=15)
        parserMocks['findOALink'].assert_not_called()
        parserMocks['validateURI'].assert_called_once()

    def test_validateAltLinkFormats_false(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(SpringerParser,
            findOALink=mocker.DEFAULT,
            validateURI=mocker.DEFAULT
        )
        parserMocks['validateURI'].return_value = True

        mockResp = mocker.MagicMock(headers={})
        mockHead = mocker.patch.object(requests, 'head')
        mockHead.return_value = mockResp

        assert testParser.validateAltLinkFormats() is False
        mockHead.assert_called_once_with('http://link.springer.com/book/10.007/1', timeout=15)
        parserMocks['findOALink'].assert_not_called()
        parserMocks['validateURI'].assert_not_called()

    def test_validateAltLinkFormats_false_bad_link(self, testParser, mocker):
        def setURI():
            testParser.uri = 'other.springer.com/about-us'

        parserMocks = mocker.patch.multiple(SpringerParser,
            findOALink=mocker.DEFAULT,
            validateURI=mocker.DEFAULT
        )
        parserMocks['findOALink'].side_effect = setURI
        parserMocks['validateURI'].return_value = True

        testParser.uri = 'springer.com/gp/book/9781234567890'

        assert testParser.validateAltLinkFormats() is False
        parserMocks['findOALink'].assert_called_once()
        parserMocks['validateURI'].assert_not_called()

    def test_findOALink(self, testParser, testSpringerPage, mocker):
        mockResp = mocker.MagicMock(status_code=200, text=testSpringerPage)
        mockGet = mocker.patch.object(requests, 'get')
        mockGet.return_value = mockResp

        testParser.findOALink()

        assert testParser.uri == 'http://link.springer.com/978-3-642-20897-3'

    def test_createLinks(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(SpringerParser,
            generateS3Root=mocker.DEFAULT,
            createPDFLinks=mocker.DEFAULT,
            createEPubLinks=mocker.DEFAULT
        )
        parserMocks['generateS3Root'].return_value = 'testRoot/'
        parserMocks['createPDFLinks'].return_value = ['pdf1', 'pdf2']
        parserMocks['createEPubLinks'].return_value = ['epub1', 'epub2']

        testLinks = testParser.createLinks()

        assert testLinks == ['pdf1', 'pdf2', 'epub1', 'epub2']
        parserMocks['generateS3Root'].assert_called_once()
        parserMocks['createPDFLinks'].assert_called_once_with('testRoot/')
        parserMocks['createEPubLinks'].assert_called_once_with('testRoot/')

    def test_createPDFLinks(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(SpringerParser,
            generateManifest=mocker.DEFAULT,
            checkAvailability=mocker.DEFAULT
        )
        parserMocks['generateManifest'].return_value = 'testManifestJSON'
        parserMocks['checkAvailability'].return_value = True

        testParser.code = '10.007'
        testParser.uriIdentifier = '1'
        testLinks = testParser.createPDFLinks('testRoot/')

        assert testLinks == [
            ('testRoot/manifests/springer/10-007_1.json', {'reader': True}, 'application/webpub+json', ('manifests/springer/10-007_1.json', 'testManifestJSON'), None),
            ('https://link.springer.com/content/pdf/10.007/1.pdf', {'download': True}, 'application/pdf', None, None)
        ]
        parserMocks['generateManifest'].assert_called_once_with('https://link.springer.com/content/pdf/10.007/1.pdf', 'testRoot/manifests/springer/10-007_1.json')
        parserMocks['checkAvailability'].assert_called_once_with('https://link.springer.com/content/pdf/10.007/1.pdf')

    def test_createPDFLinks_missing(self, testParser, mocker):
        mockCheck = mocker.patch.object(SpringerParser, 'checkAvailability')
        mockCheck.return_value = False

        assert testParser.createPDFLinks('testRoot/') == []

    def test_createEPubLinks(self, testParser, mocker):
        mockCheck = mocker.patch.object(SpringerParser, 'checkAvailability')
        mockCheck.return_value = True

        testParser.code = '10.007'
        testParser.uriIdentifier = '1'
        testLinks = testParser.createEPubLinks('testRoot/')

        assert testLinks == [
            ('testRoot/epubs/springer/10-007_1/manifest.json', {'reader': True}, 'application/webpub+json', None, None),
            ('testRoot/epubs/springer/10-007_1/META-INF/container.xml', {'reader': True}, 'application/epub+zip', None, None),
            ('testRoot/epubs/springer/10-007_1.epub', {'download': True}, 'application/epub+xml', None, ('epubs/springer/10-007_1.epub', 'https://link.springer.com/download/epub/10.007/1.epub')),
        ]
        mockCheck.assert_called_once_with('https://link.springer.com/download/epub/10.007/1.epub')

    def test_createEPubLinks_missing(self, testParser, mocker):
        mockCheck = mocker.patch.object(SpringerParser, 'checkAvailability')
        mockCheck.return_value = False

        assert testParser.createEPubLinks('testRoot/') == []

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
        mockResp = mocker.MagicMock(status_code=200)
        mockHead = mocker.patch.object(requests, 'head')
        mockHead.return_value = mockResp

        assert SpringerParser.checkAvailability('testURL') == True
