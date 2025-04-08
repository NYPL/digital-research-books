import pytest
import requests

from managers.parsers import DeGruyterParser


class TestDeGruyterParser:
    @pytest.fixture
    def test_parser(self, mocker):
        mock_record = mocker.MagicMock(
            title='test_record',
            source='test_source',
            identifiers=['1|test', '2|other']
        )
        return DeGruyterParser('www.degruyter.com/book/1', 'test_type', mock_record)

    def test_initializer(self, test_parser):
        assert test_parser.uri_identifier == None

    def test_validate_uri_true(self, test_parser):
        assert test_parser.validate_uri() is True

    def test_validate_uri_false(self, test_parser):
        test_parser.uri = 'other_uri'
        assert test_parser.validate_uri() is False

    def test_create_links_isbn_match(self, test_parser, mocker):
        parserMocks = mocker.patch.multiple(DeGruyterParser,
            generate_s3_root=mocker.DEFAULT,
            generate_pdf_links=mocker.DEFAULT,
            generate_epub_links=mocker.DEFAULT
        )

        parserMocks['generate_s3_root'].return_value = 'test_root/'
        parserMocks['generate_pdf_links'].return_value = ['pdf1', None]
        parserMocks['generate_epub_links'].return_value = ['epub1', 'epub2']

        test_parser.uri = 'degruyter.com/document/doi/10.000/9781234567890/html'

        test_links = test_parser.create_links()

        assert test_links == ['pdf1', 'epub1', 'epub2']
        parserMocks['generate_s3_root'].assert_called_once()
        parserMocks['generate_pdf_links'].assert_called_once_with(
            'test_root/', 'https://www.degruyter.com/document/doi/10.000/9781234567890/pdf'
        )
        parserMocks['generate_epub_links'].assert_called_once_with(
            'test_root/', 'https://www.degruyter.com/document/doi/10.000/9781234567890/epub'
        )

    def test_create_links_title_match(self, test_parser, mocker):
        parserMocks = mocker.patch.multiple(DeGruyterParser,
            generate_s3_root=mocker.DEFAULT,
            generate_pdf_links=mocker.DEFAULT,
            generate_epub_links=mocker.DEFAULT
        )

        parserMocks['generate_s3_root'].return_value = 'test_root/'
        parserMocks['generate_pdf_links'].return_value = ['pdf1', None]
        parserMocks['generate_epub_links'].return_value = ['epub1', 'epub2']

        test_parser.uri = 'degruyter.com/title/1'

        test_links = test_parser.create_links()

        assert test_links == ['pdf1', 'epub1', 'epub2']
        parserMocks['generate_s3_root'].assert_called_once()
        parserMocks['generate_pdf_links'].assert_called_once_with(
            'test_root/', 'https://www.degruyter.com/downloadpdf/title/1'
        )
        parserMocks['generate_epub_links'].assert_called_once_with(
            'test_root/', 'https://www.degruyter.com/downloadepub/title/1'
        )

    def test_create_links_no_match(self, test_parser, mocker):
        mock_generate = mocker.patch.object(DeGruyterParser, 'generate_s3_root')
        mock_generate.return_value = 'test_root/'

        mock_abstract_create = mocker.patch('managers.parsers.parser_abc.ParserABC.create_links')
        mock_abstract_create.return_value = ['test_link']

        test_links = test_parser.create_links()

        assert test_links == ['test_link']
        mock_generate.assert_called_once()
        mock_abstract_create.assert_called_once()

    def test_generate_epub_links_success(self, test_parser, mocker):
        mock_head = mocker.patch.object(DeGruyterParser, 'make_head_query')
        mock_head.return_value = (200, 'mock_header')

        test_parser.uri_identifier = 1
        test_links = test_parser.generate_epub_links('test_root/', 'epub_source_uri')

        assert test_links == [
            ('test_root/epubs/degruyter/1/manifest.json', {'reader': True}, 'application/webpub+json', None, None),
            ('test_root/epubs/degruyter/1/META-INF/container.xml', {'reader': True}, 'application/epub+xml', None, None),
            ('test_root/epubs/degruyter/1.epub', {'download': True}, 'application/epub+zip', None, ('epubs/degruyter/1.epub', 'epub_source_uri'))
        ]
        mock_head.assert_called_once_with('epub_source_uri')

    def test_generate_epub_links_error(self, test_parser, mocker):
        mock_head = mocker.patch.object(DeGruyterParser, 'make_head_query')
        mock_head.return_value = (500, 'mock_header')

        test_parser.uri_identifier = 1
        test_links = test_parser.generate_epub_links('test_root/', 'epub_source_uri')

        assert test_links == [None]
        mock_head.assert_called_once_with('epub_source_uri')

    def test_generate_pdf_links(self, test_parser, mocker):
        mock_generate = mocker.patch.object(DeGruyterParser, 'generate_manifest')
        mock_generate.return_value = 'test_manifest_json'

        test_parser.uri_identifier = 1
        test_links = test_parser.generate_pdf_links('test_root/', 'pdf_source_uri')

        assert test_links == [
            ('test_root/manifests/degruyter/1.json', {'reader': True}, 'application/webpub+json', ('manifests/degruyter/1.json', 'test_manifest_json'), None),
            ('pdf_source_uri', {'download': True}, 'application/pdf', None, None)
        ]

    def test_generate_manifest(self, test_parser, mocker):
        mock_abstract_manifest = mocker.patch('managers.parsers.parser_abc.ParserABC.generate_manifest')
        mock_abstract_manifest.return_value = 'test_manifest'

        assert test_parser.generate_manifest('source_uri', 'manifest_uri') == 'test_manifest'
        mock_abstract_manifest.assert_called_once_with('source_uri', 'manifest_uri')

    def test_generate_s3_root(self, test_parser, mocker):
        mock_abstract_generate = mocker.patch('managers.parsers.parser_abc.ParserABC.generate_s3_root')
        mock_abstract_generate.return_value = 'test_root'

        assert test_parser.generate_s3_root() == 'test_root'
        mock_abstract_generate.assert_called_once()

    def test_make_head_query(self, mocker):
        mock_response = mocker.MagicMock()
        mock_response.status_code = 200
        mock_response.headers = 'test_headers'

        mock_head = mocker.patch.object(requests, 'head')
        mock_head.return_value = mock_response

        assert DeGruyterParser.make_head_query('test_uri') == (200, 'test_headers')
        mock_head.assert_called_once_with(
            'test_uri', timeout=15, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5)'}
        )
