import os
import pytest
from flask import Flask
from sqlalchemy.orm.exc import NoResultFound
from api.blueprints.drbCollection import (
    collectionCreate,
    get_collection,
    collectionReplace,
    collectionUpdate,
    collectionDelete,
    collectionDeleteWorkEdition,
    get_collections,
    constructSortMethod,
    constructOPDSFeed,
    validateToken,
)
from api.utils import APIUtils
from api.opdsUtils import OPDSUtils


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
    mock_db_client = mocker.patch("api.blueprints.drbCollection.DBClient")
    mock_db_client.return_value = mock_db
    return mock_db, mock_db_client


@pytest.fixture(autouse=True)
def mock_b64decode(mocker):
    mock = mocker.patch("api.blueprints.drbCollection.b64decode")
    mock.return_value = b"testUser:testPswd"
    return mock


@pytest.fixture(autouse=True)
def set_env(mocker):
    mocker.patch.dict(
        os.environ,
        {
            "NYPL_API_CLIENT_PUBLIC_KEY": "test",
            "ENVIRONMENT": "test",
            "ELASTICSEARCH_INDEX": "test_es_index",
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
    mock_db, mock_db_client = mock_db_and_client
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
        assert mock_db_client.call_count == 2
        assert mock_db.createSession.call_count == 2
        assert mockRemoveEdition.call_count == 1
        mock_db.fetchSingleCollection.assert_called_once_with("testUUID")
        mock_db.session.commit.assert_called_once()
        mock_feed_construct.assert_called_once_with(collection, mock_db)
        mock_utils["formatOPDS2Object"].assert_called_once_with(200, "testOPDS2Feed")


def test_collection_delete_work_edition_error(
    test_app, mock_utils, mocker, mock_db_and_client
):
    mock_db, mock_db_client = mock_db_and_client
    mock_db.fetchSingleCollection.return_value = mocker.MagicMock(uuid="testUUID")
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"
    mock_utils["validatePassword"].return_value = True

    with test_app.test_request_context(
        "/delete/testUUID", headers={"Authorization": "Basic testAuth"}
    ):
        test_api_response = collectionDeleteWorkEdition("testUUID")

        assert test_api_response == "testErrorResponse"
        assert mock_db_client.call_count == 1
        assert mock_db.createSession.call_count == 1
        mock_utils["formatResponseObject"].assert_called_once_with(
            400,
            "deleteCollectionWorkEdition",
            {
                "message": "At least one of these fields(editionIDs & workUUIDs) are required"
            },
        )
