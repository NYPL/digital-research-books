import pytest

from managers.parsers import MDPIParser


class TestMDPIParser:
    @pytest.fixture
    def test_parser(self, mocker):
        mock_record = mocker.MagicMock(
            title="testRecord", source="testSource", identifiers=["1|test", "2|other"]
        )
        return MDPIParser(
            "mdpi.com/books/pdfview/book/1", "application/pdf", mock_record
        )

    def test_initializer(self, test_parser):
        assert test_parser.identifier == "1"

    def test_validate_uri_true(self, test_parser):
        assert test_parser.validate_uri() is True

    def test_validate_uri_false(self, test_parser):
        test_parser.uri = "otherURI"
        assert test_parser.validate_uri() is False

    def test_create_links(self, test_parser, mocker):
        parser_mocks = mocker.patch.multiple(
            MDPIParser,
            generate_s3_root=mocker.DEFAULT,
            generate_pdf_links=mocker.DEFAULT,
        )

        parser_mocks["generate_s3_root"].return_value = "test_root/"
        parser_mocks["generate_pdf_links"].return_value = ["pdf1", "pdf2"]

        test_links = test_parser.create_links()

        assert test_links == ["pdf1", "pdf2"]
        parser_mocks["generate_s3_root"].assert_called_once()
        parser_mocks["generate_pdf_links"].assert_called_once_with("test_root/")

    def test_generate_pdf_links(self, test_parser, mocker):
        mock_generate = mocker.patch.object(MDPIParser, "generate_manifest")
        mock_generate.return_value = "test_manifest_json"

        test_links = test_parser.generate_pdf_links("test_root/")

        assert test_links == [
            (
                "test_root/manifests/mdpi/1.json",
                {"reader": True},
                "application/webpub+json",
                ("manifests/mdpi/1.json", "test_manifest_json"),
                None,
            ),
            (
                "http://mdpi.com/books/pdfdownload/book/1",
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
