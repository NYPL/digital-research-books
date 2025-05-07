import pytest
import requests
from requests.exceptions import ReadTimeout

from managers.parsers import FrontierParser


class TestFrontierParser:
    @pytest.fixture
    def test_parser(self, mocker):
        mock_record = mocker.MagicMock(
            title="testRecord", source="testSource", identifiers=["1|test", "2|other"]
        )
        return FrontierParser(
            "www.frontiersin.org/research-topics/1/1", "testType", mock_record
        )

    def test_initializer(self, test_parser):
        assert test_parser.uri_identifier == None

    def test_validate_uri_true(self, test_parser):
        assert test_parser.validate_uri() is True
        assert test_parser.uri_identifier == "1"

    def test_validate_uri_false(self, test_parser):
        test_parser.uri = "other_uri"
        assert test_parser.validate_uri() is False

    def test_create_links(self, test_parser, mocker):
        parser_mocks = mocker.patch.multiple(
            FrontierParser,
            generate_s3_root=mocker.DEFAULT,
            generate_pdf_links=mocker.DEFAULT,
            generate_epub_links=mocker.DEFAULT,
        )

        parser_mocks["generate_s3_root"].return_value = "test_root/"
        parser_mocks["generate_pdf_links"].return_value = ["pdf1", None]
        parser_mocks["generate_epub_links"].return_value = ["epub1", "epub2"]

        test_links = test_parser.create_links()

        assert test_links == ["pdf1", "epub1", "epub2"]
        parser_mocks["generate_s3_root"].assert_called_once()
        parser_mocks["generate_pdf_links"].assert_called_once_with("test_root/")
        parser_mocks["generate_epub_links"].assert_called_once_with("test_root/")

    def test_generate_epub_links_success(self, test_parser, mocker):
        mock_check = mocker.patch.object(FrontierParser, "check_availability")
        mock_check.return_value = (200, {"content-disposition": "filename=title.EPUB"})

        test_parser.uri_identifier = 1
        test_links = test_parser.generate_epub_links("test_root/")

        assert test_links == [
            (
                "test_root/epubs/frontier/1_title/manifest.json",
                {"reader": True},
                "application/webpub+json",
                None,
                None,
            ),
            (
                "test_root/epubs/frontier/1_title/META-INF/container.xml",
                {"reader": True},
                "application/epub+xml",
                None,
                None,
            ),
            (
                "test_root/epubs/frontier/1_title.epub",
                {"download": True},
                "application/epub+zip",
                None,
                (
                    "epubs/frontier/1_title.epub",
                    "https://www.frontiersin.org/research-topics/1/epub",
                ),
            ),
        ]
        mock_check.assert_called_once_with(
            "https://www.frontiersin.org/research-topics/1/epub"
        )

    def test_generate_epub_links_request_error(self, test_parser, mocker):
        mock_check = mocker.patch.object(FrontierParser, "check_availability")
        mock_check.return_value = (500, {})

        test_parser.uri_identifier = 1
        test_links = test_parser.generate_epub_links("test_root/")

        assert test_links == [None]

    def test_generate_epub_links_header_error(self, test_parser, mocker):
        mock_resp = mocker.MagicMock(status_code=200, headers={"other": "value"})
        mock_head = mocker.patch.object(requests, "head")
        mock_head.return_value = mock_resp

        test_parser.uri_identifier = 1
        test_links = test_parser.generate_epub_links("test_root/")

        assert test_links == [None]

    def test_generate_pdf_links(self, test_parser, mocker):
        mock_generate = mocker.patch.object(FrontierParser, "generate_manifest")
        mock_generate.return_value = "test_manifest_json"

        test_parser.uri_identifier = 1
        test_links = test_parser.generate_pdf_links("test_root/")

        assert test_links == [
            (
                "test_root/manifests/frontier/1.json",
                {"reader": True},
                "application/webpub+json",
                ("manifests/frontier/1.json", "test_manifest_json"),
                None,
            ),
            (
                "https://www.frontiersin.org/research-topics/1/pdf",
                {"download": True},
                "application/pdf",
                None,
                None,
            ),
        ]

    def test_generate_manifest(self, test_parser, mocker):
        mock_abstract_manifest = mocker.patch(
            "managers.parsers.parser_abc.ParserABC.generate_manifest"
        )
        mock_abstract_manifest.return_value = "test_manifest"

        assert (
            test_parser.generate_manifest("source_uri", "manifest_uri")
            == "test_manifest"
        )
        mock_abstract_manifest.assert_called_once_with("source_uri", "manifest_uri")

    def test_generate_s3_root(self, test_parser, mocker):
        mock_abstract_generate = mocker.patch(
            "managers.parsers.parser_abc.ParserABC.generate_s3_root"
        )
        mock_abstract_generate.return_value = "test_root"

        assert test_parser.generate_s3_root() == "test_root"
        mock_abstract_generate.assert_called_once()

    def test_check_availability(self, mocker):
        mock_resp = mocker.MagicMock(status_code=200, headers="test_headers")
        mock_head = mocker.patch.object(requests, "head")
        mock_head.return_value = mock_resp

        assert FrontierParser.check_availability("test_url") == (200, "test_headers")

    def test_check_availability_timeout(self, mocker):
        mock_head = mocker.patch.object(requests, "head")
        mock_head.side_effect = ReadTimeout

        assert FrontierParser.check_availability("test_url") == (0, None)
