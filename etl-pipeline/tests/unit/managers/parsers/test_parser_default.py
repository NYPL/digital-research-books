import pytest

from managers.parsers import DefaultParser
from tests.helper import TestHelpers


class TestDefaultParser:
    @classmethod
    def setup_class(cls):
        TestHelpers.setEnvVars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clearEnvVars()

    @pytest.fixture
    def test_parser(self, mocker):
        mock_record = mocker.MagicMock(
            title="test_record", source="test_source", identifiers=["1|test", "2|other"]
        )
        return DefaultParser("test_uri", "test_type", mock_record)

    def test_initializer(self, test_parser):
        assert test_parser.source == "test_source"
        assert test_parser.identifier == "1"

    def test_validate_uri(self, test_parser, mocker):
        mock_abstract_validate = mocker.patch(
            "managers.parsers.parser_abc.ParserABC.validate_uri"
        )
        mock_abstract_validate.return_value = True

        assert test_parser.validate_uri() is True
        mock_abstract_validate.assert_called_once()

    def test_create_links_pdf(self, test_parser, mocker):
        parser_mocks = mocker.patch.multiple(
            DefaultParser,
            generate_s3_root=mocker.DEFAULT,
            generate_manifest=mocker.DEFAULT,
        )

        parser_mocks["generate_s3_root"].return_value = "test_root/"
        parser_mocks["generate_manifest"].return_value = "test_manifest_json"

        test_parser.media_type = "application/pdf"

        test_links = test_parser.create_links()

        assert test_links == [
            (
                "test_root/manifests/test_source/1.json",
                {"reader": True},
                "application/webpub+json",
                ("manifests/test_source/1.json", "test_manifest_json"),
                None,
            ),
            (
                "http://test_uri",
                {"reader": False, "download": True},
                "application/pdf",
                None,
                None,
            ),
        ]
        parser_mocks["generate_s3_root"].assert_called_once()
        parser_mocks["generate_manifest"].assert_called_once_with(
            "http://test_uri", "test_root/manifests/test_source/1.json"
        )

    def test_create_links_epub(self, test_parser, mocker):
        mock_generate = mocker.patch.object(DefaultParser, "generate_s3_root")
        mock_generate.return_value = "test_root/"

        test_parser.media_type = "application/epub+zip"

        test_links = test_parser.create_links()

        assert test_links == [
            (
                "test_root/epubs/test_source/1/manifest.json",
                {"reader": True},
                "application/webpub+json",
                None,
                None,
            ),
            (
                "test_root/epubs/test_source/1/META-INF/container.xml",
                {"reader": True},
                "application/epub+xml",
                None,
                None,
            ),
            (
                "test_root/epubs/test_source/1.epub",
                {"download": True},
                "application/epub+zip",
                None,
                ("epubs/test_source/1.epub", "http://test_uri"),
            ),
        ]
        mock_generate.assert_called_once()

    def test_create_links_other(self, test_parser, mocker):
        mock_abstract_create = mocker.patch(
            "managers.parsers.parser_abc.ParserABC.create_links"
        )
        mock_abstract_create.return_value = "test_links"

        assert test_parser.create_links() == "test_links"
        mock_abstract_create.assert_called_once()

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
