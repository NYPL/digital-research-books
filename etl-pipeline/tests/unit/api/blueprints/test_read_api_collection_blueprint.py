import os
import pytest
from flask import Flask
from sqlalchemy.orm.exc import NoResultFound
from api.blueprints.drbCollection import (
    get_collection,
    get_collections,
)
from api.utils import APIUtils
from api.opdsUtils import OPDSUtils
from api.decorators import require_basic_authentication


@pytest.fixture
def mock_utils(mocker):
    return mocker.patch.multiple(
        APIUtils,
        formatResponseObject=mocker.DEFAULT,
        formatOPDS2Object=mocker.DEFAULT,
        validatePassword=mocker.DEFAULT,
    )


@pytest.fixture
def mock_db_and_client(mocker):
    mock_db = mocker.MagicMock(session=mocker.MagicMock())
    mock_db.fetchUser.return_value = mocker.MagicMock(
        user="testUser", password="testPswd", salt="testSalt"
    )
    mock_db_client_decorators = mocker.patch("api.decorators.DBClient")
    mock_db_client_decorators.return_value = mock_db
    mock_db_client_drbCollection = mocker.patch("api.blueprints.drbCollection.DBClient")
    mock_db_client_drbCollection.return_value = mock_db
    return mock_db, mock_db_client_decorators, mock_db_client_drbCollection


@pytest.fixture(autouse=True)
def mock_b64decode(mocker):
    mock = mocker.patch("api.decorators.b64decode")
    mock.return_value = b"testUser:testPswd"
    return mock


@pytest.fixture(autouse=True)
def set_env(mocker):
    mocker.patch.dict(
        os.environ,
        {
            "NYPL_API_CLIENT_PUBLIC_KEY": "test",
            "ENVIRONMENT": "test",
            "DRB_ELASTICSEARCH_INDEX": "test_es_index",
        },
    )


@pytest.fixture
def test_app():
    flask_app = Flask("test")
    flask_app.config["SQL_ENGINE"] = "testDBClient"
    return flask_app


def test_get_collection_success(test_app, mock_utils, mocker, mock_db_and_client):
    mock_db = mock_db_and_client[0]
    collection = mocker.MagicMock(uuid="d902fd44-7cbe-4401-b50c-5b1bda8b1059")
    mock_db.fetchSingleCollection.return_value = collection
    mock_feed_construct = mocker.patch("api.blueprints.drbCollection.constructOPDSFeed")
    mock_feed_construct.return_value = "testOPDS2Feed"
    mock_utils["formatOPDS2Object"].return_value = "testOPDS2Response"

    with test_app.test_request_context("/?sort=title&page=3"):
        test_api_response = get_collection("d902fd44-7cbe-4401-b50c-5b1bda8b1059")

        assert test_api_response == "testOPDS2Response"
        mock_db.createSession.assert_called_once()
        mock_feed_construct.assert_called_once_with(
            collection, mock_db, sort="title", page=3, perPage=10
        )
        mock_utils["formatOPDS2Object"].assert_called_once_with(200, "testOPDS2Feed")


def test_get_collection_not_found(test_app, mock_utils, mock_db_and_client):
    mock_db = mock_db_and_client[0]
    mock_db.fetchSingleCollection.side_effect = NoResultFound
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"

    with test_app.test_request_context("/?sort=title&page=3"):
        test_api_response = get_collection("d902fd44-7cbe-4401-b50c-5b1bda8b1059")

        assert test_api_response == "testErrorResponse"
        mock_db.createSession.assert_called_once()
        mock_utils["formatResponseObject"].assert_called_once_with(
            404,
            "fetchCollection",
            {
                "message": "No collection found with id d902fd44-7cbe-4401-b50c-5b1bda8b1059"
            },
        )


def test_get_collection_error(test_app, mock_utils, mock_db_and_client):
    mock_db = mock_db_and_client[0]
    mock_db.fetchSingleCollection.side_effect = Exception("Database error")
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"

    with test_app.test_request_context("/?sort=title&page=3"):
        test_api_response = get_collection("d902fd44-7cbe-4401-b50c-5b1bda8b1059")

        assert test_api_response == "testErrorResponse"
        mock_db.createSession.assert_called_once()
        mock_utils["formatResponseObject"].assert_called_once_with(
            500,
            "fetchCollection",
            {
                "message": "Unable to get collection with id d902fd44-7cbe-4401-b50c-5b1bda8b1059"
            },
        )


def test_get_collection_invalid_id(test_app, mock_utils):
    mock_utils["formatResponseObject"].return_value = "400response"

    with test_app.test_request_context("/?sort=title&page=3"):
        test_api_response = get_collection("testUUID")

        assert test_api_response == "400response"
        mock_utils["formatResponseObject"].assert_called_once_with(
            400, "fetchCollection", {"message": "Collection id testUUID is invalid"}
        )


def test_get_collections_success(test_app, mock_utils, mocker, mock_db_and_client):
    mock_db = mock_db_and_client[0]
    collection1 = mocker.MagicMock(uuid="uuid1")
    collection2 = mocker.MagicMock(uuid="uuid2")
    mock_db.fetchCollections.return_value = [collection1, collection2]
    mock_feed = mocker.MagicMock()
    mock_feed_init = mocker.patch("api.blueprints.drbCollection.Feed")
    mock_feed_init.return_value = mock_feed
    mock_paging = mocker.patch.object(OPDSUtils, "addPagingOptions")
    mockConstruct = mocker.patch("api.blueprints.drbCollection.constructOPDSFeed")
    mockConstruct.side_effect = ["group1", "group2"]
    mock_utils["formatOPDS2Object"].return_value = "testOPDSResponse"

    with test_app.test_request_context("/list"):
        test_response = get_collections()

        assert test_response == "testOPDSResponse"
        mock_db.createSession.assert_called_once()
        mock_db.fetchCollections.assert_called_once_with(
            sort="title", page=1, perPage=10
        )
        mock_feed_init.assert_called_once()
        mock_feed.addMetadata.assert_called_once_with(
            {"title": "Digital Research Books Collections"}
        )
        mock_feed.addLink.assert_called_once_with(
            {"rel": "self", "href": "/list", "type": "application/opds+json"}
        )
        mock_paging.assert_called_once_with(mock_feed, "/list?", 2, page=1, perPage=10)
        mockConstruct.assert_has_calls(
            [
                mocker.call(
                    collection1,
                    mock_db,
                    perPage=5,
                    path="/collection/uuid1",
                    build_publications=False,
                ),
                mocker.call(
                    collection2,
                    mock_db,
                    perPage=5,
                    path="/collection/uuid2",
                    build_publications=False,
                ),
            ]
        )
        mock_feed.addGroup.assert_has_calls(
            [mocker.call("group1"), mocker.call("group2")]
        )
        mock_utils["formatOPDS2Object"].assert_called_once_with(200, mock_feed)


def test_get_collections_error(test_app, mock_utils, mock_db_and_client):
    mock_db = mock_db_and_client[0]
    mock_db.fetchCollections.side_effect = Exception("Database error")
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"

    with test_app.test_request_context("/list"):
        test_response = get_collections()

        assert test_response == "testErrorResponse"
        mock_db.createSession.assert_called_once()
        mock_db.fetchCollections.assert_called_once_with(
            sort="title", page=1, perPage=10
        )
        mock_utils["formatResponseObject"].assert_called_once_with(
            500, "collectionList", {"message": "Unable to get collections"}
        )


def test_get_collections_sort_error(test_app, mock_utils):
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"

    with test_app.test_request_context("/list?sort=error"):
        test_response = get_collections()

        assert test_response == "testErrorResponse"
        mock_utils["formatResponseObject"].assert_called_once_with(
            400, "collectionList", {"message": "Sort fields are invalid"}
        )


def test_validate_token_success(
    test_app, mock_utils, mocker, mock_db_and_client, mock_b64decode
):
    mock_func = mocker.MagicMock()
    decorated_function = require_basic_authentication(mock_func)
    mock_utils["validatePassword"].return_value = True

    with test_app.test_request_context(
        "/", headers={"Authorization": "Basic testAuth"}
    ):
        decorated_function()

        mock_b64decode.assert_called_once_with(b"testAuth")
        mock_func.assert_called_once_with(user="testUser")


def test_validate_token_header_error(test_app, mock_utils, mocker):
    mock_func = mocker.MagicMock()
    decorated_function = require_basic_authentication(mock_func)
    mock_utils["formatResponseObject"].return_value = "testError"

    with test_app.test_request_context("/"):
        test_response = decorated_function()

        assert test_response == "testError"
        mock_utils["formatResponseObject"].assert_called_once_with(
            403, "authResponse", {"message": "user/password not provided"}
        )


def test_validate_token_auth_error(
    test_app, mock_utils, mocker, mock_db_and_client, mock_b64decode
):
    mock_func = mocker.MagicMock()
    decorated_function = require_basic_authentication(mock_func)
    mock_utils["validatePassword"].return_value = False
    mock_utils["formatResponseObject"].return_value = "testError"

    with test_app.test_request_context(
        "/", headers={"Authorization": "Basic testAuth"}
    ):
        test_response = decorated_function()

        assert test_response == "testError"
        mock_utils["formatResponseObject"].assert_called_once_with(
            401, "authResponse", {"message": "invalid user/password"}
        )
