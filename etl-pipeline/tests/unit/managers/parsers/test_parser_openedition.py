import pytest
import requests

from managers.parsers import OpenEditionParser


class TestOpenEditionParser:
    @pytest.fixture
    def test_parser(self, mocker):
        mock_record = mocker.MagicMock(
            title="testRecord", source="testSource", identifiers=["1|test", "2|other"]
        )
        return OpenEditionParser(
            "books.openedition.org/p123/1", "testType", mock_record
        )

    @pytest.fixture
    def test_oe_page(self):
        return open("./tests/fixtures/openeditions_book_14472.html", "r").read()

    def test_initializer(self, test_parser):
        assert test_parser.uri_identifier == None
        assert test_parser.publisher == None

    def test_validate_uri_true(self, test_parser):
        assert test_parser.validate_uri() is True
        assert test_parser.uri_identifier == "1"
        assert test_parser.publisher == "p123"

    def test_validate_uri_true_split(self, test_parser):
        test_parser.uri = "Leader text: books.openedition.org/p123/1"

        assert test_parser.validate_uri() is True
        assert test_parser.uri == "http://books.openedition.org/p123/1"
        assert test_parser.uri_identifier == "1"
        assert test_parser.publisher == "p123"

    def test_validate_uri_false(self, test_parser):
        test_parser.uri = "otherURI"
        assert test_parser.validate_uri() is False

    def test_create_links_epub(self, test_parser, mocker):
        parser_mocks = mocker.patch.multiple(
            OpenEditionParser,
            load_ebook_links=mocker.DEFAULT,
            create_epub_link=mocker.DEFAULT,
        )
        parser_mocks["load_ebook_links"].return_value = [
            ("epubURI", "application/epub+xml", "testFlags")
        ]
        parser_mocks["create_epub_link"].return_value = ["epubXML", "epubZIP"]

        test_links = test_parser.create_links()

        assert test_links == ["epubXML", "epubZIP"]
        parser_mocks["load_ebook_links"].assert_called_once()
        parser_mocks["create_epub_link"].assert_called_once_with(
            "epubURI", "application/epub+xml", "testFlags"
        )

    def test_create_links_pdf(self, test_parser, mocker):
        parser_mocks = mocker.patch.multiple(
            OpenEditionParser,
            load_ebook_links=mocker.DEFAULT,
            create_pdf_link=mocker.DEFAULT,
        )
        parser_mocks["load_ebook_links"].return_value = [
            ("pdfURI", "application/webpub+json", "testFlags")
        ]
        parser_mocks["create_pdf_link"].return_value = ["pdfJSON", "pdfSource"]

        test_links = test_parser.create_links()

        assert test_links == ["pdfJSON", "pdfSource"]
        parser_mocks["load_ebook_links"].assert_called_once()
        parser_mocks["create_pdf_link"].assert_called_once_with(
            "pdfURI", "application/webpub+json", "testFlags"
        )

    def test_create_links_other(self, test_parser, mocker):
        mock_load = mocker.patch.object(OpenEditionParser, "load_ebook_links")
        mock_load.return_value = [("otherURI", "testFlags", "type/test")]

        test_links = test_parser.create_links()

        assert test_links == [("otherURI", "type/test", "testFlags", None, None)]
        mock_load.assert_called_once()

    def test_create_epub_link(self, test_parser, mocker):
        mock_root = mocker.patch.object(OpenEditionParser, "generate_s3_root")
        mock_root.return_value = "testRoot/"

        test_parser.publisher = "pub"
        test_parser.uri_identifier = 1
        test_links = test_parser.create_epub_link(
            "sourceURI", "application/epub+xml", {"reader": True}
        )

        assert test_links == [
            (
                "testRoot/epubs/doab/pub_1/manifest.json",
                {"reader": True},
                "application/webpub+json",
                None,
                None,
            ),
            (
                "testRoot/epubs/doab/pub_1/META-INF/container.xml",
                {"reader": True},
                "application/epub+xml",
                None,
                None,
            ),
            (
                "testRoot/epubs/doab/pub_1.epub",
                {"download": True},
                "application/epub+zip",
                None,
                ("epubs/doab/pub_1.epub", "sourceURI"),
            ),
        ]

        mock_root.assert_called_once()

    def test_create_pdf_link(self, test_parser, mocker):
        mock_root = mocker.patch.object(OpenEditionParser, "generate_s3_root")
        mock_root.return_value = "testRoot/"

        mock_generate = mocker.patch.object(OpenEditionParser, "generate_manifest")
        mock_generate.return_value = "testManifestJSON"

        test_parser.publisher = "pub"
        test_parser.uri_identifier = 1
        test_links = test_parser.create_pdf_link(
            "sourceURI", "application/webpub+json", {"reader": True}
        )

        assert test_links == [
            (
                "testRoot/manifests/doab/pub_1.json",
                {"reader": True},
                "application/webpub+json",
                ("manifests/doab/pub_1.json", "testManifestJSON"),
                None,
            ),
            ("sourceURI", {"download": True}, "application/pdf", None, None),
        ]

    def test_load_ebook_links_success(self, test_parser, test_oe_page, mocker):
        mock_resp = mocker.MagicMock(status_code=200, text=test_oe_page)
        mock_get = mocker.patch.object(requests, "get")
        mock_get.return_value = mock_resp

        mock_parse = mocker.patch.object(OpenEditionParser, "parse_book_link")
        mock_parse.side_effect = ["openAccess", None, "pdf", "epub"]

        test_links = test_parser.load_ebook_links()

        assert test_links == ["openAccess", "pdf", "epub"]
        assert "14482" == mock_parse.mock_calls[0].args[0].get("href")
        assert "14482?format=reader" == mock_parse.mock_calls[1].args[0].get("href")
        assert "http://books.openedition.org/cths/epub/14472" == mock_parse.mock_calls[
            2
        ].args[0].get("href")
        assert "http://books.openedition.org/cths/pdf/14472" == mock_parse.mock_calls[
            3
        ].args[0].get("href")

    def test_load_ebook_links_error(self, test_parser, test_oe_page, mocker):
        mock_resp = mocker.MagicMock(status_code=500, text="errorPage")
        mock_get = mocker.patch.object(requests, "get")
        mock_get.return_value = mock_resp

        assert test_parser.load_ebook_links() == []

    def test_parse_book_link(self, test_parser, mocker):
        mock_link = mocker.MagicMock()
        mock_link.get.return_value = "12345"

        test_parser.publisher = "test"
        test_link = test_parser.parse_book_link(mock_link)

        assert test_link == ("http://books.openedition.org/test/12345", "text/html", {})
        mock_link.get.assert_called_once_with("href")

    def test_generate_manifest(self, test_parser, mocker):
        mock_abstract_manifest = mocker.patch(
            "managers.parsers.parser_abc.ParserABC.generate_manifest"
        )
        mock_abstract_manifest.return_value = "testManifest"

        assert (
            test_parser.generate_manifest("sourceURI", "manifestURI") == "testManifest"
        )
        mock_abstract_manifest.assert_called_once_with("sourceURI", "manifestURI")

    def test_generate_s3_root(self, test_parser, mocker):
        mock_abstract_generate = mocker.patch(
            "managers.parsers.parser_abc.ParserABC.generate_s3_root"
        )
        mock_abstract_generate.return_value = "testRoot"

        assert test_parser.generate_s3_root() == "testRoot"
        mock_abstract_generate.assert_called_once()
