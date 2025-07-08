import pytest
import requests
from requests import ConnectionError

from managers import DOABLinkManager
from managers.doab_parser import LinkError


class TestDOABLinkManager:
    @pytest.fixture
    def test_manager(self, mocker):
        return DOABLinkManager(mocker.MagicMock())

    @pytest.fixture
    def test_has_parts(self):
        return [
            '1|test_uri|test|test_type|{"test": true}',
            '2|testOtherURI|test|testOtherType|{"test": false}',
        ]

    def test_load_parsers(self, test_manager):
        assert len(test_manager.parsers) == 7
        assert test_manager.parsers[0].__name__ == "SpringerParser"
        assert test_manager.parsers[6].__name__ == "DefaultParser"

    def test_select_parser(self, test_manager, mocker):
        mock_find_uri = mocker.patch.object(DOABLinkManager, "find_final_uri")
        mock_find_uri.return_value = ("test_root", "test_type")

        test_manager.parsers = []
        parser_instances = []
        for i in range(3):
            mock_instance = mocker.MagicMock()
            mock_instance.validate_uri.return_value = True if i == 1 else False

            mock_parser = mocker.MagicMock()
            mock_parser.return_value = mock_instance

            parser_instances.append(mock_instance)
            test_manager.parsers.append(mock_parser)

        test_parser = test_manager.select_parser("test_uri", "test_type")

        assert test_parser == parser_instances[1]
        for i in range(3):
            test_manager.parsers[1].assert_called_once_with(
                "test_root", "test_type", test_manager.record
            )
        parser_instances[2].validateURI.assert_not_called

    def test_parse_links(self, test_manager, test_has_parts, mocker):
        test_manager.record.has_part = test_has_parts

        mock_parser = mocker.MagicMock(uri="testSourceURI")
        mock_parser.create_links.return_value = [
            ("parseURI1", {"other": True}, "test_type1", "testManifest", None),
            ("parseURI2", {"other": False}, "test_type2", None, "testEPub"),
        ]

        mock_select = mocker.patch.object(DOABLinkManager, "select_parser")
        mock_select.side_effect = [mock_parser] * 2

        test_manager.parse_links()

        assert test_manager.record.has_part == [
            '1|parseURI1|test|test_type1|{"test": true, "other": true}',
            '1|parseURI2|test|test_type2|{"test": true, "other": false}',
        ]
        assert test_manager.manifests == ["testManifest"]
        assert test_manager.epub_links == ["testEPub"]

    def test_parse_links_error(self, test_manager, test_has_parts, mocker):
        test_manager.record.has_part = test_has_parts

        mock_select = mocker.patch.object(DOABLinkManager, "select_parser")
        mock_select.side_effect = [LinkError("test")] * 2

        test_manager.parse_links()

        assert test_manager.record.has_part == []

    def test_find_final_uri_direct(self, mocker):
        mock_response = mocker.MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "text/html; utf-8"}

        mock_head = mocker.patch.object(requests, "head")
        mock_head.return_value = mock_response

        test_uri, test_type = DOABLinkManager.find_final_uri("test_uri", "test_type")

        assert test_uri == "test_uri"
        assert test_type == "text/html"
        mock_head.assert_called_once_with("test_uri", allow_redirects=False, timeout=15)

    def test_find_final_uri_error(self, mocker):
        mock_head = mocker.patch.object(requests, "head")
        mock_head.side_effect = ConnectionError

        with pytest.raises(LinkError):
            DOABLinkManager.find_final_uri("test_uri", "test_type")

    def test_find_final_uri_redirect(self, mocker):
        mock_response = mocker.MagicMock()
        mock_response.status_code = 301
        mock_response.headers = {"Location": "sourceURI"}

        mock_response2 = mocker.MagicMock()
        mock_response2.status_code = 200
        mock_response2.headers = {"Content-Type": "application/test"}

        mock_head = mocker.patch.object(requests, "head")
        mock_head.side_effect = [mock_response, mock_response2]

        test_uri, test_type = DOABLinkManager.find_final_uri("test_uri", "test_type")

        assert test_uri == "sourceURI"
        assert test_type == "application/test"
        mock_head.assert_has_calls(
            [
                mocker.call("test_uri", allow_redirects=False, timeout=15),
                mocker.call("sourceURI", allow_redirects=False, timeout=15),
            ]
        )
