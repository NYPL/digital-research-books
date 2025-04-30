import pytest

from managers.parsers.parser_abc import ParserABC
from tests.helper import TestHelpers


class TestParserABC:
    @classmethod
    def setup_class(cls):
        TestHelpers.setEnvVars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clearEnvVars()

    @pytest.fixture
    def test_parser(self, mocker):
        class TestParser(ParserABC):
            def __init__(self, uri, media_type, record):
                super().__init__(uri, media_type, record)

            def validate_uri(self):
                return super().validate_uri()

            def create_links(self):
                return super().create_links()

            def generate_manifest(self, source_uri, manifest_uri):
                return super().generate_manifest(source_uri, manifest_uri)

            def generate_s3_root(self):
                return super().generate_s3_root()

        return TestParser("test_uri", "testType", mocker.MagicMock(title="testRecord"))

    def test_uri_setter(self, test_parser):
        assert test_parser.uri == "http://test_uri"

        test_parser.uri = "https://new_uri"

        assert test_parser.uri == "https://new_uri"

    def test_validate_uri(self, test_parser):
        assert test_parser.validate_uri() is True

    def test_create_links(self, test_parser):
        assert test_parser.create_links() == [
            ("http://test_uri", None, "testType", None, None)
        ]

    def test_generate_manifest(self, test_parser, mocker):
        mock_manifest = mocker.MagicMock(links=[])
        mock_manifest.toJson.return_value = "json_manifest"

        mock_manifest_manager = mocker.patch(
            "managers.parsers.parser_abc.WebpubManifest"
        )
        mock_manifest_manager.return_value = mock_manifest

        test_manifest = test_parser.generate_manifest("source_uri", "manifest_uri")

        assert test_manifest == "json_manifest"
        assert mock_manifest.links == [
            {"rel": "self", "href": "manifest_uri", "type": "application/webpub+json"}
        ]
        mock_manifest_manager.assert_called_once_with("source_uri", "application/pdf")
        mock_manifest.addMetadata.assert_called_once_with(test_parser.record)
        mock_manifest.addChapter.assert_called_once_with("source_uri", "testRecord")
        mock_manifest.toJson.assert_called_once()

    def test_generate_s3_root(self, test_parser):
        assert (
            test_parser.generate_s3_root()
            == "https://test_aws_bucket.s3.amazonaws.com/"
        )
