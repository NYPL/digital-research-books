from flask import Flask, request
import pytest

from api.blueprints.drbWork import get_work
from api.utils import APIUtils


class TestWorkBlueprint:
    @pytest.fixture
    def mock_utils(self, mocker):
        return mocker.patch.multiple(
            APIUtils,
            normalizeQueryParams=mocker.DEFAULT,
            extractParamPairs=mocker.DEFAULT,
            formatResponseObject=mocker.DEFAULT,
            formatWorkOutput=mocker.DEFAULT,
        )

    @pytest.fixture
    def test_app(self):
        flask_app = Flask("test")
        flask_app.config["DB_CLIENT"] = "testDBClient"
        flask_app.config["READER_VERSION"] = "test"

        return flask_app

    def test_get_work_success_show_all_true(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbWork.DBClient", return_value=mock_db
        )

        mock_utils["normalizeQueryParams"].return_value = {"showAll": ["true"]}

        mock_db.fetchSingleWork.return_value = "dbWorkRecord"

        mock_utils["formatWorkOutput"].return_value = "testWork"
        mock_utils["formatResponseObject"].return_value = "singleWorkResponse"

        with test_app.test_request_context("/?showAll=true"):
            test_api_response = get_work("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "singleWorkResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["normalizeQueryParams"].assert_called_once
            mock_utils["formatWorkOutput"].assert_called_once_with(
                "dbWorkRecord",
                None,
                showAll=True,
                dbClient=mock_db,
                formats=[],
                reader="test",
                request=request,
            )
            mock_utils["formatResponseObject"].assert_called_once_with(
                200, "singleWork", "testWork"
            )

    def test_get_work_success_format(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbWork.DBClient", return_value=mock_db
        )

        query_params = {"showAll": ["true"]}
        mock_utils["normalizeQueryParams"].return_value = {"showAll": ["true"]}

        mock_utils["extractParamPairs"].side_effect = [[("format", "requestable")]]

        mock_db.fetchSingleWork.return_value = "dbWorkRecord"

        mock_utils["formatWorkOutput"].return_value = "testWork"
        mock_utils["formatResponseObject"].return_value = "singleWorkResponse"

        with test_app.test_request_context("/?showAll=true"):
            test_api_response = get_work("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "singleWorkResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["normalizeQueryParams"].assert_called_once
            mock_utils["extractParamPairs"].assert_has_calls(
                [mocker.call("filter", query_params)]
            )
            mock_utils["formatWorkOutput"].assert_called_once_with(
                "dbWorkRecord",
                None,
                showAll=True,
                dbClient=mock_db,
                formats=["application/html+edd", "application/x.html+edd"],
                reader="test",
                request=request,
            )
            mock_utils["formatResponseObject"].assert_called_once_with(
                200, "singleWork", "testWork"
            )

    def test_get_work_success_show_all_false(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbWork.DBClient", return_value=mock_db
        )

        mock_utils["normalizeQueryParams"].return_value = {"showAll": ["false"]}

        mock_db.fetchSingleWork.return_value = "dbWorkRecord"

        mock_utils["formatWorkOutput"].return_value = "testWork"
        mock_utils["formatResponseObject"].return_value = "singleWorkResponse"

        with test_app.test_request_context("/?showAll=false"):
            test_api_response = get_work("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "singleWorkResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["normalizeQueryParams"].assert_called_once
            mock_utils["formatWorkOutput"].assert_called_once_with(
                "dbWorkRecord",
                None,
                showAll=False,
                dbClient=mock_db,
                formats=[],
                reader="test",
                request=request,
            )
            mock_utils["formatResponseObject"].assert_called_once_with(
                200, "singleWork", "testWork"
            )

    def test_get_work_missing(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbWork.DBClient", return_value=mock_db
        )

        mock_utils["normalizeQueryParams"].return_value = {}

        mock_db.fetchSingleWork.return_value = None

        mock_utils["formatResponseObject"].return_value = "404Response"

        with test_app.test_request_context(
            "/a8512b02-779b-45c6-95a3-56f90831be46?showAll=false"
        ):
            test_api_response = get_work("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "404Response"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["normalizeQueryParams"].assert_called_once()
            mock_utils["formatWorkOutput"].assert_not_called()
            mock_utils["formatResponseObject"].assert_called_once_with(
                404,
                "singleWork",
                {
                    "message": "No work found with id a8512b02-779b-45c6-95a3-56f90831be46"
                },
            )

    def test_get_work_missing(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbWork.DBClient", return_value=mock_db
        )

        mock_utils["normalizeQueryParams"].return_value = {}

        mock_db.fetchSingleWork.side_effect = Exception("Database error")

        mock_utils["formatResponseObject"].return_value = "500Response"

        with test_app.test_request_context(
            "/a8512b02-779b-45c6-95a3-56f90831be46?showAll=false"
        ):
            test_api_response = get_work("a8512b02-779b-45c6-95a3-56f90831be46")

            assert test_api_response == "500Response"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["formatResponseObject"].assert_called_once_with(
                500,
                "singleWork",
                {
                    "message": "Unable to get work with id a8512b02-779b-45c6-95a3-56f90831be46"
                },
            )

    def test_get_work_invalid_id(self, mock_utils, test_app):
        mock_utils["formatResponseObject"].return_value = "400Response"

        with test_app.test_request_context("/testUUID?showAll=false"):
            test_api_response = get_work("testUUID")

            assert test_api_response == "400Response"

            mock_utils["formatResponseObject"].assert_called_once_with(
                400, "singleWork", {"message": "Work id testUUID is invalid"}
            )
