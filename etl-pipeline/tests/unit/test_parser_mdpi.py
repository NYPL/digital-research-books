import pytest

from managers.parsers import MDPIParser


class TestMDPIParser:
    @pytest.fixture
    def testParser(self, mocker):
        mockRecord = mocker.MagicMock(
            title='testRecord',
            source='testSource',
            identifiers=['1|test', '2|other']
        )
        return MDPIParser('mdpi.com/books/pdfview/book/1', 'application/pdf', mockRecord)

    def test_initializer(self, testParser):
        assert testParser.identifier == '1'

    def test_validateURI_true(self, testParser):
        assert testParser.validateURI() is True

    def test_validateURI_false(self, testParser):
        testParser.uri = 'otherURI'
        assert testParser.validateURI() is False

    def test_createLinks(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(MDPIParser,
            generateS3Root=mocker.DEFAULT,
            generatePDFLinks=mocker.DEFAULT,
        )

        parserMocks['generateS3Root'].return_value = 'testRoot/'
        parserMocks['generatePDFLinks'].return_value = ['pdf1', 'pdf2']

        testLinks = testParser.createLinks()

        assert testLinks == ['pdf1', 'pdf2']
        parserMocks['generateS3Root'].assert_called_once()
        parserMocks['generatePDFLinks'].assert_called_once_with('testRoot/')

    def test_generatePDFLinks(self, testParser, mocker):
        mockGenerate = mocker.patch.object(MDPIParser, 'generateManifest')
        mockGenerate.return_value = 'testManifestJSON'

        testLinks = testParser.generatePDFLinks('testRoot/')

        assert testLinks == [
            ('testRoot/manifests/mdpi/1.json', {'reader': True}, 'application/webpub+json', ('manifests/mdpi/1.json', 'testManifestJSON'), None),
            ('http://mdpi.com/books/pdfdownload/book/1', {'download': True}, 'application/pdf', None, None)
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
