import pytest

from managers.parsers import DefaultParser


class TestDefaultParser:
    @pytest.fixture
    def testParser(self, mocker):
        mockRecord = mocker.MagicMock(
            title='testRecord',
            source='testSource',
            identifiers=['1|test', '2|other']
        )
        return DefaultParser('testURI', 'testType', mockRecord)

    def test_initializer(self, testParser):
        assert testParser.source == 'testSource'
        assert testParser.identifier == '1'

    def test_validateURI(self, testParser, mocker):
        mockAbstractValidate = mocker.patch('managers.parsers.ParserABC.ParserABC.validateURI')
        mockAbstractValidate.return_value = True

        assert testParser.validateURI() is True
        mockAbstractValidate.assert_called_once()

    def test_createLinks_pdf(self, testParser, mocker):
        parserMocks = mocker.patch.multiple(DefaultParser,
            generateS3Root=mocker.DEFAULT,
            generateManifest=mocker.DEFAULT
        )

        parserMocks['generateS3Root'].return_value = 'testRoot/'
        parserMocks['generateManifest'].return_value = 'testManifestJSON'

        testParser.mediaType = 'application/pdf'

        testLinks = testParser.createLinks()

        assert testLinks == [
            ('testRoot/manifests/testSource/1.json', {'reader': True}, 'application/webpub+json', ('manifests/testSource/1.json', 'testManifestJSON'), None),
            ('http://testURI', {'reader': False, 'download': True}, 'application/pdf', None, None)
        ]
        parserMocks['generateS3Root'].assert_called_once()
        parserMocks['generateManifest'].assert_called_once_with('http://testURI', 'testRoot/manifests/testSource/1.json')

    def test_createLinks_epub(self, testParser, mocker):
        mockGenerate = mocker.patch.object(DefaultParser, 'generateS3Root')
        mockGenerate.return_value = 'testRoot/'

        testParser.mediaType = 'application/epub+zip'

        testLinks = testParser.createLinks()

        assert testLinks == [
            ('testRoot/epubs/testSource/1/manifest.json', {'reader': True}, 'application/webpub+json', None, None),
            ('testRoot/epubs/testSource/1/META-INF/container.xml', {'reader': True}, 'application/epub+xml', None, None),
            ('testRoot/epubs/testSource/1.epub', {'download': True}, 'application/epub+zip', None, ('epubs/testSource/1.epub', 'http://testURI'))
        ]
        mockGenerate.assert_called_once()

    def test_createLinks_other(self, testParser, mocker):
        mockAbstractCreate = mocker.patch('managers.parsers.ParserABC.ParserABC.createLinks')
        mockAbstractCreate.return_value = 'testLinks'

        assert testParser.createLinks() == 'testLinks'
        mockAbstractCreate.assert_called_once()

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
