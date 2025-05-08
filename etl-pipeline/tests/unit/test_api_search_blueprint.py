from flask import Flask, request
import pytest

from api.blueprints.drbSearch import query
from api.utils import APIUtils
from api.elastic import ElasticClientError


class TestSearchBlueprint:
    @pytest.fixture
    def mock_utils(self, mocker):
        return mocker.patch.multiple(
            APIUtils,
            normalizeQueryParams=mocker.DEFAULT,
            extractParamPairs=mocker.DEFAULT,
            formatAggregationResult=mocker.DEFAULT,
            formatPagingOptions=mocker.DEFAULT,
            formatWorkOutput=mocker.DEFAULT,
            formatResponseObject=mocker.DEFAULT,
        )

    @pytest.fixture
    def fake_hit(self, mocker):
        class FakeHit:
            def __init__(self, uuid, edition_ids):
                self.uuid = uuid
                editions = []
                for edition_id in edition_ids:
                    ed = mocker.MagicMock()
                    ed.edition_id = edition_id
                    editions.append(ed)
                mock_beta = mocker.MagicMock()
                mock_beta.inner_hits.editions.hits = editions
                if int(uuid[-1]) % 2 != 0:
                    mock_beta.highlight = {"field": ["highlight_{}".format(uuid)]}
                self.meta = mock_beta

        return FakeHit

    @pytest.fixture
    def mock_hits(self, fake_hit, mocker):
        class FakeHits:
            def __init__(self):
                self.total = mocker.MagicMock(value=5)
                self.hits = [
                    fake_hit("uuid1", ["ed1", "ed2"]),
                    fake_hit("uuid2", ["ed3"]),
                    fake_hit("uuid3", ["ed4", "ed5", "ed6"]),
                    fake_hit("uuid4", ["ed7"]),
                    fake_hit("uuid5", ["ed8"]),
                ]

            def __iter__(self):
                for h in self.hits:
                    yield h

        return FakeHits()

    @pytest.fixture
    def test_app(self):
        flask_app = Flask("test")
        flask_app.config["DB_CLIENT"] = "testDBClient"
        flask_app.config["REDIS_CLIENT"] = "testRedisClient"
        flask_app.config["READER_VERSION"] = "test"

        return flask_app

    def test_query(self, mock_utils, mock_hits, test_app, mocker):
        mock_es = mocker.MagicMock()
        mock_es_client = mocker.patch("api.blueprints.drbSearch.ElasticClient")
        mock_es_client.return_value = mock_es

        mock_db = mocker.MagicMock()
        mock_db_client = mocker.patch("api.blueprints.drbSearch.DBClient")
        mock_db_client.return_value = mock_db

        query_params = {"query": ["q1", "q2"], "sort": ["s1"], "size": [5]}
        mock_utils["normalizeQueryParams"].return_value = query_params

        mock_utils["extractParamPairs"].side_effect = [
            ["testQueryTerms"],
            ["testSortTerms"],
            [("format", "html")],
            ["testShowAll"],
        ]
        mock_utils["formatAggregationResult"].return_value = "testFacets"
        mock_utils["formatPagingOptions"].return_value = "testPaging"

        mock_response = mocker.MagicMock()
        mock_response.hits = mock_hits
        mock_agg = mocker.MagicMock()
        mock_agg.to_dict.return_value = {"aggs": []}
        mock_response.aggregations = mock_agg
        mock_es.searchQuery.return_value = mock_response

        mock_db.fetchSearchedWorks.return_value = [
            "work1",
            "work2",
            "work3",
            "work4",
            "work5",
        ]

        mock_utils["formatWorkOutput"].return_value = "testWorks"

        mock_utils["formatResponseObject"].return_value = "mockAPIResponse"

        with test_app.test_request_context("/?testing=true"):
            test_api_response = query()

            assert test_api_response == "mockAPIResponse"
            mock_es_client.assert_called_once_with("testRedisClient")
            mock_db_client.assert_called_once_with("testDBClient")

            mock_utils["normalizeQueryParams"].assert_called_once
            mock_utils["extractParamPairs"].assert_has_calls(
                [
                    mocker.call("query", query_params),
                    mocker.call("sort", query_params),
                    mocker.call("filter", query_params),
                    mocker.call("showAll", query_params),
                ]
            )
            mock_es.searchQuery.assert_called_once_with(
                {
                    "query": ["testQueryTerms"],
                    "sort": ["testSortTerms"],
                    "filter": [("format", "html"), "testShowAll"],
                },
                page=0,
                perPage=5,
            )
            test_result_ids = [
                ("uuid1", ["ed1", "ed2"], {"field": ["highlight_uuid1"]}),
                ("uuid2", ["ed3"], {}),
                ("uuid3", ["ed4", "ed5", "ed6"], {"field": ["highlight_uuid3"]}),
                ("uuid4", ["ed7"], {}),
                ("uuid5", ["ed8"], {"field": ["highlight_uuid5"]}),
            ]
            mock_db.fetchSearchedWorks.assert_called_once_with(test_result_ids)
            mock_utils["formatAggregationResult"].assert_called_once_with({"aggs": []})
            mock_utils["formatPagingOptions"].assert_called_once_with(1, 5, 5)
            mock_utils["formatWorkOutput"].assert_called_once_with(
                ["work1", "work2", "work3", "work4", "work5"],
                test_result_ids,
                dbClient=mock_db,
                formats=["text/html"],
                reader="test",
                request=request,
            )

            mock_utils["formatResponseObject"].assert_called_once_with(
                200,
                "searchResponse",
                {
                    "totalWorks": 5,
                    "works": "testWorks",
                    "paging": "testPaging",
                    "facets": "testFacets",
                },
            )

    def test_query_elastic_error(self, mock_utils, test_app, mocker):
        mock_es = mocker.MagicMock()
        mock_es_client = mocker.patch("api.blueprints.drbSearch.ElasticClient")
        mock_es_client.return_value = mock_es

        mocker.patch("api.blueprints.drbSearch.DBClient")

        query_params = {"query": ["q1", "q2"], "sort": ["s1"], "size": [5]}
        mock_utils["normalizeQueryParams"].return_value = query_params

        mock_utils["extractParamPairs"].side_effect = [
            ["testQueryTerms"],
            ["testSortTerms"],
            ["testFilterTerms"],
            ["testShowAll"],
        ]

        mock_utils["formatResponseObject"].return_value = "mockAPIResponse"

        mock_es.searchQuery.side_effect = ElasticClientError("Test Exception")

        with test_app.test_request_context("/?testing=true"):
            test_api_response = query()

            assert test_api_response == "mockAPIResponse"

            mock_utils["formatResponseObject"].assert_called_once_with(
                500, "searchResponse", {"message": "Unable to execute search"}
            )

    def test_query_db_error(self, mock_utils, mock_hits, test_app, mocker):
        mock_es = mocker.MagicMock()
        mock_es_client = mocker.patch("api.blueprints.drbSearch.ElasticClient")
        mock_es_client.return_value = mock_es

        mock_db = mocker.MagicMock()
        mock_db_client = mocker.patch("api.blueprints.drbSearch.DBClient")
        mock_db_client.return_value = mock_db

        query_params = {"query": ["q1", "q2"], "sort": ["s1"], "size": [5]}
        mock_utils["normalizeQueryParams"].return_value = query_params

        mock_utils["extractParamPairs"].side_effect = [
            ["testQueryTerms"],
            ["testSortTerms"],
            [("format", "html")],
            ["testShowAll"],
        ]
        mock_utils["formatAggregationResult"].return_value = "testFacets"
        mock_utils["formatPagingOptions"].return_value = "testPaging"
        mock_utils["formatResponseObject"].return_value = "mockAPIResponse"

        mock_db.fetchSearchedWorks.side_effect = Exception("Database error")

        with test_app.test_request_context("/?testing=true"):
            test_api_response = query()

            assert test_api_response == "mockAPIResponse"

            mock_utils["formatResponseObject"].assert_called_once_with(
                500, "searchResponse", {"message": "Unable to execute search"}
            )
