import os
import pytest
from flask import Flask
from api.blueprints.drbCollection import (
    collectionDelete,
    collectionDeleteWorkEdition,
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
def test_app():
    flask_app = Flask("test")
    flask_app.config["DB_CLIENT"] = "testDBClient"
    return flask_app


def test_collection_delete_success(test_app, mock_utils, mock_db_and_client):
    mock_db = mock_db_and_client[0]
    mock_db.deleteCollection.return_value = 1
    mock_utils["validatePassword"].return_value = True

    with test_app.test_request_context(
        "/", headers={"Authorization": "Bearer testToken"}
    ):
        test_api_response = collectionDelete("testUUID")

        assert test_api_response[0].status_code == 200
        assert test_api_response[0].json == {"message": "Deleted testUUID"}
        assert mock_db.createSession.call_count == 2
        mock_db.deleteCollection.assert_called_once_with("testUUID")
        mock_db.session.commit.assert_called_once()


def test_collection_delete_error(test_app, mock_utils, mock_db_and_client):
    mock_db = mock_db_and_client[0]
    mock_db.deleteCollection.return_value = 0
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"
    mock_utils["validatePassword"].return_value = True

    with test_app.test_request_context(
        "/", headers={"Authorization": "Bearer testToken"}
    ):
        test_api_response = collectionDelete("testUUID")

        assert test_api_response == "testErrorResponse"
        assert mock_db.createSession.call_count == 2
        mock_db.deleteCollection.assert_called_once_with("testUUID")
        mock_utils["formatResponseObject"].assert_called_once_with(
            404,
            "deleteCollection",
            {"message": "No collection with UUID testUUID exists"},
        )


def test_collection_delete_work_edition_success(
    test_app, mock_utils, mocker, mock_db_and_client
):
    mock_db, mock_db_client_decorators, mock_db_client_drbCollection = (
        mock_db_and_client
    )
    collection = mocker.MagicMock(uuid="testUUID")
    mock_db.fetchSingleCollection.return_value = collection
    mock_feed_construct = mocker.patch("api.blueprints.drbCollection.constructOPDSFeed")
    mock_feed_construct.return_value = "testOPDS2Feed"
    mockRemoveEdition = mocker.patch(
        "api.blueprints.drbCollection.removeWorkEditionsFromCollection"
    )
    mock_utils["formatOPDS2Object"].return_value = "testOPDS2Response"
    mock_utils["validatePassword"].return_value = True

    with test_app.test_request_context(
        "/delete/testUUID?editionIDs=testID",
        headers={"Authorization": "Basic testAuth"},
    ):
        test_api_response = collectionDeleteWorkEdition("testUUID")

        assert test_api_response == "testOPDS2Response"
        assert mock_db_client_decorators.call_count == 1
        assert mock_db_client_drbCollection.call_count == 1
        assert mock_db.createSession.call_count == 2
        assert mockRemoveEdition.call_count == 1
        mock_db.fetchSingleCollection.assert_called_once_with("testUUID")
        mock_db.session.commit.assert_called_once()
        mock_feed_construct.assert_called_once_with(collection, mock_db)
        mock_utils["formatOPDS2Object"].assert_called_once_with(200, "testOPDS2Feed")


def test_collection_delete_work_edition_error(
    test_app, mock_utils, mocker, mock_db_and_client
):
    mock_db, mock_db_client_decorators, _ = mock_db_and_client
    mock_db.fetchSingleCollection.return_value = mocker.MagicMock(uuid="testUUID")
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"
    mock_utils["validatePassword"].return_value = True

    with test_app.test_request_context(
        "/delete/testUUID", headers={"Authorization": "Basic testAuth"}
    ):
        test_api_response = collectionDeleteWorkEdition("testUUID")

        assert test_api_response == "testErrorResponse"
        assert mock_db_client_decorators.call_count == 1
        assert mock_db.createSession.call_count == 1
        mock_utils["formatResponseObject"].assert_called_once_with(
            400,
            "deleteCollectionWorkEdition",
            {
                "message": "At least one of these fields(editionIDs & workUUIDs) are required"
            },
        )
