import pytest
import requests

from managers.parsers import SpringerParser


class TestSpringerParser:
    @pytest.fixture
    def test_parser(self, mocker):
        mock_record = mocker.MagicMock(
            title="testRecord", source="testSource", identifiers=["1|test", "2|other"]
        )
        return SpringerParser(
            "link.springer.com/book/10.007/1", "testType", mock_record
        )

    @pytest.fixture
    def test_springer_page(self):
        return open("./tests/fixtures/springer_book_9783642208973.html", "r").read()

    def test_initializer(self, test_parser):
        assert test_parser.uri_identifier == None
        assert test_parser.code == None

    def test_validate_uri_true(self, test_parser):
        assert test_parser.validate_uri() is True
        assert test_parser.uri_identifier == "1"
        assert test_parser.code == "10.007"

    def test_validate_alt_link_formats_true(self, test_parser, mocker):
        mock_validate_alt = mocker.patch.object(
            SpringerParser, "validate_alt_link_formats"
        )
        mock_validate_alt.return_value = True

        test_parser.uri = "springer.com/gp/book/978123456789"

        assert test_parser.validate_uri() is True
        assert test_parser.uri == "http://springer.com/gp/book/978123456789"

    def test_validate_uri_false(self, test_parser):
        test_parser.uri = "otherURI"
        assert test_parser.validate_uri() is False

    def test_validate_alt_link_formats_true(self, test_parser, mocker):
        parser_mocks = mocker.patch.multiple(
            SpringerParser, find_oa_link=mocker.DEFAULT, validate_uri=mocker.DEFAULT
        )
        parser_mocks["validate_uri"].return_value = True

        mock_resp = mocker.MagicMock(headers={"Location": "finalURI"})
        mock_head = mocker.patch.object(requests, "head")
        mock_head.return_value = mock_resp

        assert test_parser.validate_alt_link_formats() is True
        assert test_parser.uri == "http://finalURI"
        mock_head.assert_called_once_with(
            "http://link.springer.com/book/10.007/1", timeout=15
        )
        parser_mocks["find_oa_link"].assert_not_called()
        parser_mocks["validate_uri"].assert_called_once()

    def test_validate_alt_link_formats_false(self, test_parser, mocker):
        parser_mocks = mocker.patch.multiple(
            SpringerParser, find_oa_link=mocker.DEFAULT, validate_uri=mocker.DEFAULT
        )
        parser_mocks["validate_uri"].return_value = True

        mock_resp = mocker.MagicMock(headers={})
        mock_head = mocker.patch.object(requests, "head")
        mock_head.return_value = mock_resp

        assert test_parser.validate_alt_link_formats() is False
        mock_head.assert_called_once_with(
            "http://link.springer.com/book/10.007/1", timeout=15
        )
        parser_mocks["find_oa_link"].assert_not_called()
        parser_mocks["validate_uri"].assert_not_called()

    def test_validate_alt_link_formats_false_bad_link(self, test_parser, mocker):
        def set_uri():
            test_parser.uri = "other.springer.com/about-us"

        parser_mocks = mocker.patch.multiple(
            SpringerParser, find_oa_link=mocker.DEFAULT, validate_uri=mocker.DEFAULT
        )
        parser_mocks["find_oa_link"].side_effect = set_uri
        parser_mocks["validate_uri"].return_value = True

        test_parser.uri = "springer.com/gp/book/9781234567890"

        assert test_parser.validate_alt_link_formats() is False
        parser_mocks["find_oa_link"].assert_called_once()
        parser_mocks["validate_uri"].assert_not_called()

    def test_find_oa_link(self, test_parser, test_springer_page, mocker):
        mock_resp = mocker.MagicMock(status_code=200, text=test_springer_page)
        mock_get = mocker.patch.object(requests, "get")
        mock_get.return_value = mock_resp

        test_parser.find_oa_link()

        assert test_parser.uri == "http://link.springer.com/978-3-642-20897-3"

    def test_create_links(self, test_parser, mocker):
        parser_mocks = mocker.patch.multiple(
            SpringerParser,
            generate_s3_root=mocker.DEFAULT,
            create_pdf_links=mocker.DEFAULT,
            create_epub_links=mocker.DEFAULT,
        )
        parser_mocks["generate_s3_root"].return_value = "testRoot/"
        parser_mocks["create_pdf_links"].return_value = ["pdf1", "pdf2"]
        parser_mocks["create_epub_links"].return_value = ["epub1", "epub2"]

        test_links = test_parser.create_links()

        assert test_links == ["pdf1", "pdf2", "epub1", "epub2"]
        parser_mocks["generate_s3_root"].assert_called_once()
        parser_mocks["create_pdf_links"].assert_called_once_with("testRoot/")
        parser_mocks["create_epub_links"].assert_called_once_with("testRoot/")

    def test_create_pdf_links(self, test_parser, mocker):
        parser_mocks = mocker.patch.multiple(
            SpringerParser,
            generate_manifest=mocker.DEFAULT,
            check_availability=mocker.DEFAULT,
        )
        parser_mocks["generate_manifest"].return_value = "testManifestJSON"
        parser_mocks["check_availability"].return_value = True

        test_parser.code = "10.007"
        test_parser.uri_identifier = "1"
        test_links = test_parser.create_pdf_links("testRoot/")

        assert test_links == [
            (
                "testRoot/manifests/springer/10-007_1.json",
                {"reader": True},
                "application/webpub+json",
                ("manifests/springer/10-007_1.json", "testManifestJSON"),
                None,
            ),
            (
                "https://link.springer.com/content/pdf/10.007/1.pdf",
                {"download": True},
                "application/pdf",
                None,
                None,
            ),
        ]
        parser_mocks["generate_manifest"].assert_called_once_with(
            "https://link.springer.com/content/pdf/10.007/1.pdf",
            "testRoot/manifests/springer/10-007_1.json",
        )
        parser_mocks["check_availability"].assert_called_once_with(
            "https://link.springer.com/content/pdf/10.007/1.pdf"
        )

    def test_create_pdf_links_missing(self, test_parser, mocker):
        mock_check = mocker.patch.object(SpringerParser, "check_availability")
        mock_check.return_value = False

        assert test_parser.create_pdf_links("testRoot/") == []

    def test_create_epub_links(self, test_parser, mocker):
        mock_check = mocker.patch.object(SpringerParser, "check_availability")
        mock_check.return_value = True

        test_parser.code = "10.007"
        test_parser.uri_identifier = "1"
        test_links = test_parser.create_epub_links("testRoot/")

        assert test_links == [
            (
                "testRoot/epubs/springer/10-007_1/manifest.json",
                {"reader": True},
                "application/webpub+json",
                None,
                None,
            ),
            (
                "testRoot/epubs/springer/10-007_1/META-INF/container.xml",
                {"reader": True},
                "application/epub+zip",
                None,
                None,
            ),
            (
                "testRoot/epubs/springer/10-007_1.epub",
                {"download": True},
                "application/epub+xml",
                None,
                (
                    "epubs/springer/10-007_1.epub",
                    "https://link.springer.com/download/epub/10.007/1.epub",
                ),
            ),
        ]
        mock_check.assert_called_once_with(
            "https://link.springer.com/download/epub/10.007/1.epub"
        )

    def test_create_epub_links_missing(self, test_parser, mocker):
        mock_check = mocker.patch.object(SpringerParser, "check_availability")
        mock_check.return_value = False

        assert test_parser.create_epub_links("testRoot/") == []

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

    def test_check_availability(self, mocker):
        mock_resp = mocker.MagicMock(status_code=200)
        mock_head = mocker.patch.object(requests, "head")
        mock_head.return_value = mock_resp

        assert SpringerParser.check_availability("testURL") == True
