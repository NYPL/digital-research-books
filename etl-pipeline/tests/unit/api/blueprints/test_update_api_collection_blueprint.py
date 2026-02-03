import os
import pytest
from flask import Flask
from api.blueprints.drbCollection import (
    collectionReplace,
    collectionUpdate,
)
from api.utils import APIUtils


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
def collection_request_body():
    def _make(exclude=None, **overrides):
        base = {
            "title": "Test Collection",
            "creator": "Test Creator",
            "description": "Test Description",
            "workUUIDs": ["uuid1", "uuid2"],
            "editionIDs": ["ed1", "ed2", "ed3"],
            "autoDef": {
                "sortField": "date",
                "sortDirection": "ASC",
                "keywordQuery": "bikes",
            },
        }
        if exclude:
            for key in exclude:
                base.pop(key, None)
        base.update(overrides)
        return base

    return _make


@pytest.fixture
def test_app():
    flask_app = Flask("test")
    flask_app.config["DB_CLIENT"] = "testDBClient"
    return flask_app


def test_collection_replace_success(
    test_app,
    mock_utils,
    mocker,
    mock_db_and_client,
    collection_request_body,
    mock_b64decode,
):
    mock_db, mock_db_client_decorators, mock_db_client_drbCollection = (
        mock_db_and_client
    )
    collection = mocker.MagicMock(uuid="testUUID")
    mock_db.fetchSingleCollection.return_value = collection
    mock_feed_construct = mocker.patch("api.blueprints.drbCollection.constructOPDSFeed")
    mock_feed_construct.return_value = "testOPDS2Feed"
    test_updated_collection = collection_request_body(
        exclude=["description", "workUUIDs", "autoDef"],
        title="Updated Test Collection",
        creator="Updated Test Creator",
        description="Updated Test Description",
    )
    mock_utils["validatePassword"].return_value = True
    mock_utils["formatOPDS2Object"].return_value = "testOPDS2Response"

    with test_app.test_request_context(
        "/replace/testUUID",
        json=test_updated_collection,
        headers={"Authorization": "Basic testAuth"},
    ):
        test_api_response = collectionReplace("testUUID")

        assert test_api_response == "testOPDS2Response"
        assert mock_db_client_decorators.call_count == 1
        assert mock_db_client_drbCollection.call_count == 1
        assert mock_db.createSession.call_count == 2
        assert mock_db.session.execute.call_count == 1
        mock_db.fetchSingleCollection.assert_called_once_with("testUUID")
        mock_db.session.commit.assert_called_once()
        mock_feed_construct.assert_called_once_with(collection, mock_db)
        mock_b64decode.assert_called_once_with(b"testAuth")
        mock_utils["formatOPDS2Object"].assert_called_once_with(201, "testOPDS2Feed")


def test_collection_replace_error(
    test_app, mock_utils, mocker, mock_db_and_client, collection_request_body
):
    mock_db = mock_db_and_client[0]
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"
    mock_db.fetchSingleCollection.return_value = mocker.MagicMock(uuid="testUUID")
    test_fail_collection = collection_request_body(
        exclude=["description", "workUUIDs", "editionIDs", "autoDef"],
        title="Updated Test Collection",
        creator="Updated Test Creator",
    )
    mock_utils["validatePassword"].return_value = True

    with test_app.test_request_context(
        "/replace/testUUID",
        json=test_fail_collection,
        headers={"Authorization": "Basic testAuth"},
    ):
        test_api_response = collectionReplace("testUUID")

        assert test_api_response == "testErrorResponse"
        mock_utils["formatResponseObject"].assert_called_once_with(
            400,
            "createCollection",
            {
                "message": "title, creator and description fields are required, with one of workUUIDs or editionIDs to create a collection"
            },
        )


def test_collection_update_success(test_app, mock_utils, mocker, mock_db_and_client):
    mock_db, mock_db_client_decorators, mock_db_client_drbCollection = (
        mock_db_and_client
    )
    collection = mocker.MagicMock(uuid="testUUID")
    mock_db.fetchSingleCollection.return_value = collection
    mock_feed_construct = mocker.patch("api.blueprints.drbCollection.constructOPDSFeed")
    mock_feed_construct.return_value = "testOPDS2Feed"
    mock_utils["formatOPDS2Object"].return_value = "testOPDS2Response"

    with test_app.test_request_context(
        "/update/testUUID?title=newTitle",
        headers={"Authorization": "Basic testAuth"},
    ):
        test_api_response = collectionUpdate("testUUID")

        assert test_api_response == "testOPDS2Response"
        assert mock_db_client_decorators.call_count == 1
        assert mock_db_client_drbCollection.call_count == 1
        assert mock_db.createSession.call_count == 2
        mock_db.fetchSingleCollection.assert_called_once_with("testUUID")
        mock_db.session.commit.assert_called_once()
        mock_feed_construct.assert_called_once_with(collection, mock_db)
        mock_utils["formatOPDS2Object"].assert_called_once_with(200, "testOPDS2Feed")


def test_collection_update_error(test_app, mock_utils, mocker, mock_db_and_client):
    mock_db = mock_db_and_client[0]
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"
    mock_db.fetchSingleCollection.return_value = mocker.MagicMock(uuid="testUUID")

    with test_app.test_request_context(
        "/update/testUUID", headers={"Authorization": "Basic testAuth"}
    ):
        test_api_response = collectionUpdate("testUUID")

        assert test_api_response == "testErrorResponse"
        mock_utils["formatResponseObject"].assert_called_once_with(
            400,
            "updateCollection",
            {
                "message": "At least one of these fields(title, creator, description, etc.) are required"
            },
        )
