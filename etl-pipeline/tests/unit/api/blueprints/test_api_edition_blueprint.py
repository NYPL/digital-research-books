from flask import Flask, request
import pytest

from api.blueprints.drbEdition import get_edition
from api.utils import APIUtils


class TestEditionBlueprint:
    @pytest.fixture
    def mock_utils(self, mocker):
        return mocker.patch.multiple(
            APIUtils,
            normalizeQueryParams=mocker.DEFAULT,
            extractParamPairs=mocker.DEFAULT,
            formatResponseObject=mocker.DEFAULT,
            formatEditionOutput=mocker.DEFAULT,
        )

    @pytest.fixture
    def test_app(self):
        flask_app = Flask("test")
        flask_app.config["DB_CLIENT"] = "testDBClient"
        flask_app.config["READER_VERSION"] = "test"

        return flask_app

    def test_get_edition_success_no_format(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbEdition.DBClient", return_value=mock_db
        )

        mock_utils["normalizeQueryParams"].return_value = {"showAll": ["true"]}

        mock_edition = mocker.MagicMock(dcdw_uuids="testUUID")

        mock_db.fetchSingleEdition.return_value = mock_edition
        mock_db.fetchRecordsByUUID.return_value = "testRecords"

        mock_utils["formatEditionOutput"].return_value = "testEdition"
        mock_utils["formatResponseObject"].return_value = "singleEditionResponse"

        with test_app.test_request_context("/"):
            test_api_response = get_edition(1)

            assert test_api_response == "singleEditionResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["normalizeQueryParams"].assert_called_once()
            mock_utils["formatEditionOutput"].assert_called_once_with(
                mock_edition,
                records="testRecords",
                dbClient=mock_db,
                showAll=True,
                formats=[],
                reader="test",
                request=request,
            )
            mock_utils["formatResponseObject"].assert_called_once_with(
                200, "singleEdition", "testEdition"
            )

    def test_get_edition_success_format(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbEdition.DBClient", return_value=mock_db
        )

        query_params = {"showAll": ["true"]}
        mock_utils["normalizeQueryParams"].return_value = {"showAll": ["true"]}

        mock_edition = mocker.MagicMock(dcdw_uuids="testUUID")

        mock_utils["extractParamPairs"].side_effect = [[("format", "requestable")]]

        mock_db.fetchSingleEdition.return_value = mock_edition
        mock_db.fetchRecordsByUUID.return_value = "testRecords"

        mock_utils["formatEditionOutput"].return_value = "testEdition"
        mock_utils["formatResponseObject"].return_value = "singleEditionResponse"

        with test_app.test_request_context("/"):
            test_api_response = get_edition(1)

            assert test_api_response == "singleEditionResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["normalizeQueryParams"].assert_called_once()
            mock_utils["extractParamPairs"].assert_has_calls(
                [mocker.call("filter", query_params)]
            )
            mock_utils["formatEditionOutput"].assert_called_once_with(
                mock_edition,
                records="testRecords",
                showAll=True,
                dbClient=mock_db,
                request=request,
                formats=["application/html+edd", "application/x.html+edd"],
                reader="test",
            )
            mock_utils["formatResponseObject"].assert_called_once_with(
                200, "singleEdition", "testEdition"
            )

    def test_get_edition_missing(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbEdition.DBClient", return_value=mock_db
        )

        mock_utils["normalizeQueryParams"].return_value = {"showAll": ["true"]}

        mock_db.fetchSingleEdition.return_value = None

        mock_utils["formatResponseObject"].return_value = "404Response"

        with test_app.test_request_context("/"):
            test_api_response = get_edition(1)

            assert test_api_response == "404Response"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["normalizeQueryParams"].assert_called_once()
            mock_utils["formatEditionOutput"].assert_not_called()
            mock_utils["formatResponseObject"].assert_called_once_with(
                404, "singleEdition", {"message": "No edition found with id 1"}
            )

    def test_get_edition_error(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbEdition.DBClient", return_value=mock_db
        )

        mock_utils["normalizeQueryParams"].return_value = {"showAll": ["true"]}

        mock_db.fetchSingleEdition.side_effect = Exception("Database error")

        mock_utils["formatResponseObject"].return_value = "500response"

        with test_app.test_request_context("/"):
            test_api_response = get_edition(1)

            assert test_api_response == "500response"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                500, "singleEdition", {"message": "Unable to get edition with id 1"}
            )

    def test_get_edition_invalid_id_error(self, mock_utils, test_app):
        with test_app.test_request_context("/"):
            get_edition("e2d0e0aa-aa72-42a0-88fb-1aeadbec2f67")

            mock_utils["formatResponseObject"].assert_called_once_with(
                400,
                "singleEdition",
                {
                    "message": "Edition id e2d0e0aa-aa72-42a0-88fb-1aeadbec2f67 is invalid"
                },
            )
