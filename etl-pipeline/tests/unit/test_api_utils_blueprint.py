from flask import Flask, Response
import pytest
import requests

from api.blueprints.drbUtils import get_languages, get_counts, proxy_response
from api.utils import APIUtils


class TestUtilsBlueprint:
    @pytest.fixture
    def mock_utils(self, mocker):
        return mocker.patch.multiple(
            APIUtils,
            normalizeQueryParams=mocker.DEFAULT,
            formatResponseObject=mocker.DEFAULT,
            formatLanguages=mocker.DEFAULT,
            formatTotals=mocker.DEFAULT,
        )

    @pytest.fixture
    def test_app(self):
        flask_app = Flask("test")
        flask_app.config["DB_CLIENT"] = "testDBClient"
        flask_app.config["REDIS_CLIENT"] = "testRedisClient"

        return flask_app

    def test_get_languages(self, mock_utils, test_app, mocker):
        mock_es = mocker.MagicMock()
        mock_es_client = mocker.patch("api.blueprints.drbUtils.ElasticClient")
        mock_es_client.return_value = mock_es

        mock_utils["normalizeQueryParams"].return_value = {"totals": ["true"]}

        mock_es.languageQuery.return_value = mocker.MagicMock(
            aggregations=["lang1", "lang2"]
        )

        mock_utils["formatLanguages"].return_value = "testLanguageList"
        mock_utils["formatResponseObject"].return_value = "languageListResponse"

        with test_app.test_request_context("/?totals=true"):
            test_api_response = get_languages()

            assert test_api_response == "languageListResponse"
            mock_es_client.assert_called_once_with("testRedisClient")

            mock_utils["normalizeQueryParams"].assert_called_once()
            mock_es.languageQuery.assert_called_once_with(True)
            mock_utils["formatLanguages"].assert_called_once_with(
                ["lang1", "lang2"], True
            )
            mock_utils["formatResponseObject"].assert_called_once_with(
                200, "languageCounts", "testLanguageList"
            )

    def test_get_languages(self, mock_utils, test_app, mocker):
        mock_es = mocker.MagicMock()
        mock_es_client = mocker.patch("api.blueprints.drbUtils.ElasticClient")
        mock_es_client.return_value = mock_es

        mock_utils["normalizeQueryParams"].return_value = {"totals": ["true"]}

        mock_es.languageQuery.side_effect = Exception("Elastic search error")

        mock_utils["formatResponseObject"].return_value = "languageListResponse"

        with test_app.test_request_context("/?totals=true"):
            test_api_response = get_languages()

            assert test_api_response == "languageListResponse"
            mock_es_client.assert_called_once_with("testRedisClient")

            mock_utils["normalizeQueryParams"].assert_called_once()
            mock_es.languageQuery.assert_called_once_with(True)
            mock_utils["formatResponseObject"].assert_called_once_with(
                500, "languageCounts", "Unable to get language counts"
            )

    def test_get_counts(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbUtils.DBClient", return_value=mock_db
        )

        mock_db.fetchRowCounts.return_value = "testTotalResult"

        mock_utils["formatTotals"].return_value = "testTotalSummary"
        mock_utils["formatResponseObject"].return_value = "totalsResponse"

        with test_app.test_request_context("/"):
            test_api_response = get_counts()

            assert test_api_response == "totalsResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_db.fetchRowCounts.assert_called_once()
            mock_utils["formatTotals"].assert_called_once_with("testTotalResult")
            mock_utils["formatResponseObject"].assert_called_once_with(
                200, "totalCounts", "testTotalSummary"
            )

    def test_get_counts_error(self, mock_utils, test_app, mocker):
        mock_db = mocker.MagicMock()
        mock_db.__enter__.return_value = mock_db
        mock_db_client = mocker.patch(
            "api.blueprints.drbUtils.DBClient", return_value=mock_db
        )

        mock_db.fetchRowCounts.side_effect = Exception("Database error")

        mock_utils["formatResponseObject"].return_value = "totalsResponse"

        with test_app.test_request_context("/"):
            test_api_response = get_counts()

            assert test_api_response == "totalsResponse"
            mock_db_client.assert_called_once_with("testDBClient")

            mock_db.fetchRowCounts.assert_called_once()
            mock_utils["formatResponseObject"].assert_called_once_with(
                500, "totalCounts", "Unable to get total counts"
            )

    def test_proxy_response_direct_success(self, test_app, mocker):
        mock_head = mocker.patch.object(requests, "head")
        mock_head.return_value = mocker.MagicMock(status_code=200)

        mock_req = mocker.patch.object(requests, "request")
        mock_req.return_value = mocker.MagicMock(
            status_code=200,
            headers={"Content-Encoding": "block", "Media-Type": "allow"},
            content="Test Content",
        )
        with test_app.test_request_context("/?proxy_url=https://www.testURL.com"):
            test_api_response = proxy_response()

            assert isinstance(test_api_response, Response)
            assert test_api_response.status_code == 200
            assert test_api_response.response == [b"Test Content"]
            assert test_api_response.headers["Media-Type"] == "allow"
            assert test_api_response.headers["Access-Control-Allow-Origin"] == "*"

            mock_head.assert_called_once_with(
                "https://www.testURL.com", headers={"User-agent": "Mozilla/5.0"}
            )
            mock_req.assert_called_once()

    def test_proxy_response_redirect_success(self, test_app, mocker):
        mock_head = mocker.patch.object(requests, "head")
        mock_head.side_effect = [
            mocker.MagicMock(status_code=301, headers={"Location": "/redirectURL"}),
            mocker.MagicMock(status_code=200),
        ]

        mock_req = mocker.patch.object(requests, "request")
        mock_req.return_value = mocker.MagicMock(
            status_code=200,
            headers={"Content-Encoding": "block", "Media-Type": "allow"},
            content="Test Content",
        )

        with test_app.test_request_context("/?proxy_url=https://www.testURL.com"):
            test_api_response = proxy_response()

            assert isinstance(test_api_response, Response)
            assert test_api_response.status_code == 200
            assert test_api_response.response == [b"Test Content"]
            assert test_api_response.headers["Media-Type"] == "allow"

            mock_head.assert_has_calls(
                [
                    mocker.call(
                        "https://www.testURL.com", headers={"User-agent": "Mozilla/5.0"}
                    ),
                    mocker.call(
                        "https://www.testURL.com/redirectURL",
                        headers={"User-agent": "Mozilla/5.0"},
                    ),
                ]
            )
            mock_req.assert_called_once()

    def test_proxy_response_redirect_error(self, test_app, mocker):
        mock_head = mocker.patch.object(requests, "head")
        mock_head.side_effect = [
            mocker.MagicMock(status_code=404, headers={"Location": "/redirectURL"}),
        ]

        mock_req = mocker.patch.object(requests, "request")
        mock_req.return_value = mocker.MagicMock(
            status_code=200,
            headers={"Content-Encoding": "block", "Media-Type": "allow"},
            content="Test Content",
        )

        with test_app.test_request_context("/?proxy_url=https://www.testURL.com"):
            test_api_response = proxy_response()

            assert isinstance(test_api_response, Response)
            assert test_api_response.status_code == 200
            assert test_api_response.response == [b"Test Content"]
            assert test_api_response.headers["Media-Type"] == "allow"

            mock_head.assert_called_once_with(
                "https://www.testURL.com", headers={"User-agent": "Mozilla/5.0"}
            )
            mock_req.assert_called_once()

    def test_proxy_response_request_exception(self, test_app, mocker):
        mock_head = mocker.patch.object(requests, "head")
        mock_head.side_effect = [
            mocker.MagicMock(status_code=301, headers={"Location": "/redirectURL"}),
            mocker.MagicMock(status_code=200),
        ]

        mock_req = mocker.patch.object(requests, "request")
        mock_req.side_effect = Exception("Unable to make request")

        with test_app.test_request_context("/?proxy_url=https://www.testURL.com"):
            test_api_response = proxy_response()

            assert isinstance(test_api_response, Response)
            assert test_api_response.status_code == 500
            assert test_api_response.response == [b"Unable to proxy response"]

            mock_head.assert_has_calls(
                [
                    mocker.call(
                        "https://www.testURL.com", headers={"User-agent": "Mozilla/5.0"}
                    ),
                    mocker.call(
                        "https://www.testURL.com/redirectURL",
                        headers={"User-agent": "Mozilla/5.0"},
                    ),
                ]
            )
            mock_req.assert_called_once()
