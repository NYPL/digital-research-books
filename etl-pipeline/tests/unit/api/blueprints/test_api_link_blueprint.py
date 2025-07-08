from flask import Flask, request
import pytest

from api.blueprints.drbLink import get_link
from api.utils import APIUtils


class TestLinkBlueprint:
    @pytest.fixture
    def mock_utils(self, mocker):
        return mocker.patch.multiple(
            APIUtils,
            formatResponseObject=mocker.DEFAULT,
            formatLinkOutput=mocker.DEFAULT,
        )

    @pytest.fixture
    def test_app(self):
        flask_app = Flask("test")
        flask_app.config["DB_CLIENT"] = "testDBClient"

        return flask_app

    def test_get_link_success(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbLink.DBClient", return_value=mock_db
        )

        mock_db.fetchSingleLink.return_value = "dbLinkRecord"

        mock_utils["formatLinkOutput"].return_value = "testLink"
        mock_utils["formatResponseObject"].return_value = "singleLinkResponse"

        with test_app.test_request_context("/"):
            test_api_response = get_link(1)

            assert test_api_response == "singleLinkResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatLinkOutput"].assert_called_once_with(
                "dbLinkRecord", request=request
            )
            mock_utils["formatResponseObject"].assert_called_once_with(
                200, "singleLink", "testLink"
            )

    def test_get_link_missing(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbLink.DBClient", return_value=mock_db
        )

        mock_db.fetchSingleLink.return_value = None
        mock_utils["formatResponseObject"].return_value = "404Response"

        with test_app.test_request_context("/"):
            test_api_response = get_link(1)

            assert test_api_response == "404Response"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatLinkOutput"].assert_not_called()
            mock_utils["formatResponseObject"].assert_called_once_with(
                404, "singleLink", {"message": "No link found with id 1"}
            )

    def test_get_link_error(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbLink.DBClient", return_value=mock_db
        )

        mock_db.fetchSingleLink.side_effect = Exception("Database error")
        mock_utils["formatResponseObject"].return_value = "500Response"

        with test_app.test_request_context("/"):
            test_api_response = get_link(1)

            assert test_api_response == "500Response"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatLinkOutput"].assert_not_called()
            mock_utils["formatResponseObject"].assert_called_once_with(
                500, "singleLink", {"message": "Unable to get link with id 1"}
            )

    def test_get_link_invalid_id_error(self, mock_utils, test_app):
        with test_app.test_request_context("/"):
            get_link("e2d0e0aa-aa72-42a0-88fb-1aeadbec2f67")

            mock_utils["formatResponseObject"].assert_called_once_with(
                400,
                "singleLink",
                {"message": "Link id e2d0e0aa-aa72-42a0-88fb-1aeadbec2f67 is invalid"},
            )
