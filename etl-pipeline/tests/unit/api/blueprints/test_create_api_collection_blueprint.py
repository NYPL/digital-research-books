import os
import pytest
from flask import Flask
from api.blueprints.drbCollection import (
    collectionCreate,
    constructSortMethod,
    constructOPDSFeed,
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


def test_create_static_collection_success(
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
    mock_db.createStaticCollection.return_value = collection
    mock_feed_construct = mocker.patch("api.blueprints.drbCollection.constructOPDSFeed")
    mock_feed_construct.return_value = "testOPDS2Feed"
    test_request_body = collection_request_body(exclude=["autoDef"])
    mock_utils["validatePassword"].return_value = True
    mock_utils["formatOPDS2Object"].return_value = "testOPDS2Response"

    with test_app.test_request_context(
        "/", json=test_request_body, headers={"Authorization": "Basic testAuth"}
    ):
        test_api_response = collectionCreate()

        assert test_api_response == "testOPDS2Response"
        assert mock_db_client_decorators.call_count == 1
        assert mock_db_client_drbCollection.call_count == 1
        assert mock_db.createSession.call_count == 2
        mock_db.createStaticCollection.assert_called_once_with(
            "Test Collection",
            "Test Creator",
            "Test Description",
            "testUser",
            workUUIDs=["uuid1", "uuid2"],
            editionIDs=["ed1", "ed2", "ed3"],
        )
        mock_db.session.commit.assert_called_once()
        mock_feed_construct.assert_called_once_with(collection, mock_db)
        mock_b64decode.assert_called_once_with(b"testAuth")
        mock_utils["formatOPDS2Object"].assert_called_once_with(201, "testOPDS2Feed")


def test_create_automatic_collection_success(
    test_app,
    mock_utils,
    mocker,
    mock_db_and_client,
    collection_request_body,
    mock_b64decode,
):
    mock_db = mock_db_and_client[0]
    mock_db.createAutomaticCollection.return_value = mocker.MagicMock(uuid="testUUID")
    mock_feed_construct = mocker.patch("api.blueprints.drbCollection.constructOPDSFeed")
    mock_feed_construct.return_value = "testOPDS2Feed"
    test_request_body = collection_request_body(exclude=["editionIDs", "workUUIDs"])
    mock_utils["validatePassword"].return_value = True
    mock_utils["formatOPDS2Object"].return_value = "testOPDS2Response"

    with test_app.test_request_context(
        "/", json=test_request_body, headers={"Authorization": "Basic testAuth"}
    ):
        test_api_response = collectionCreate()

        assert test_api_response == "testOPDS2Response"
        mock_db.createAutomaticCollection.assert_called_once_with(
            "Test Collection",
            "Test Creator",
            "Test Description",
            owner="testUser",
            sortField="date",
            sortDirection="ASC",
            limit=None,
            keywordQuery="bikes",
            authorQuery=None,
            titleQuery=None,
            subjectQuery=None,
        )
        mock_db.session.commit.assert_called_once()
        mock_feed_construct.assert_called_once_with(
            mock_db.createAutomaticCollection.return_value,
            mock_db,
        )
        mock_b64decode.assert_called_once_with(b"testAuth")
        mock_utils["formatOPDS2Object"].assert_called_once_with(201, "testOPDS2Feed")


def test_create_automatic_collection_invalid_sort(
    test_app, mock_utils, mock_db_and_client, collection_request_body, mock_b64decode
):
    mock_db = mock_db_and_client[0]
    test_request_body = collection_request_body(
        exclude=["editionIDs", "workUUIDs"],
        autoDef={"sortField": "bad_sort_field"},
    )
    mock_utils["validatePassword"].return_value = True
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"

    with test_app.test_request_context(
        "/", json=test_request_body, headers={"Authorization": "Basic testAuth"}
    ):
        test_api_response = collectionCreate()

        assert test_api_response == "testErrorResponse"
        mock_db.createAutomaticCollection.assert_not_called()
        mock_utils["formatResponseObject"].assert_called_once_with(
            400,
            "createCollection",
            {"message": "Invalid sort field bad_sort_field"},
        )
        mock_b64decode.assert_called_once_with(b"testAuth")


def test_create_automatic_collection_invalid_fields(
    test_app, mock_utils, mock_db_and_client, collection_request_body, mock_b64decode
):
    mock_db = mock_db_and_client[0]
    test_request_body = collection_request_body(
        exclude=["workUUIDs"],
        editionIDs=[1, 2, 3],
        autoDef={"sortField": "bad_sort_field"},
    )
    mock_utils["validatePassword"].return_value = True
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"

    with test_app.test_request_context(
        "/", json=test_request_body, headers={"Authorization": "Basic testAuth"}
    ):
        test_api_response = collectionCreate()

        assert test_api_response == "testErrorResponse"
        mock_db.createAutomaticCollection.assert_not_called()
        mock_utils["formatResponseObject"].assert_called_once_with(
            400,
            "createCollection",
            {
                "message": (
                    "Cannot create a collection with both an automatic collection "
                    "definition and editionIDs or workUUIDs"
                ),
            },
        )
        mock_b64decode.assert_called_once_with(b"testAuth")


def test_create_any_collection_error(
    test_app, mocker, mock_db_and_client, mock_utils, collection_request_body
):
    mock_utils["formatResponseObject"].return_value = "testErrorResponse"
    test_request_body = collection_request_body(exclude=["title", "autoDef"])
    mock_utils["validatePassword"].return_value = True

    with test_app.test_request_context(
        "/", json=test_request_body, headers={"Authorization": "Basic testAuth"}
    ):
        test_api_response = collectionCreate()

        assert test_api_response == "testErrorResponse"
        mock_utils["formatResponseObject"].assert_called_once_with(
            400,
            "createCollection",
            {
                "message": "title, creator and description fields are required",
            },
        )


def test_construct_sort_string_asc(mocker):
    sort_method, reversed = constructSortMethod("test")
    test_sorts = [
        mocker.MagicMock(metadata=mocker.MagicMock(id=1, test="b")),
        mocker.MagicMock(metadata=mocker.MagicMock(id=2, test="A")),
        mocker.MagicMock(metadata=mocker.MagicMock(id=3, test="c")),
    ]
    sorted_list = sorted(test_sorts, key=sort_method, reverse=reversed)

    assert [x.metadata.id for x in sorted_list] == [2, 1, 3]


def test_construct_sort_int_desc(mocker):
    sort_method, reversed = constructSortMethod("test:desc")
    test_sorts = [
        mocker.MagicMock(metadata=mocker.MagicMock(id=1, test=3)),
        mocker.MagicMock(metadata=mocker.MagicMock(id=2, test=1)),
        mocker.MagicMock(metadata=mocker.MagicMock(id=3, test=2)),
    ]
    sorted_list = sorted(test_sorts, key=sort_method, reverse=reversed)

    assert [x.metadata.id for x in sorted_list] == [1, 3, 2]


def test_construct_opds_feed_static_success(test_app, mocker):
    mock_feed = mocker.MagicMock()
    mock_feed_init = mocker.patch("api.blueprints.drbCollection.Feed")
    mock_feed_init.return_value = mock_feed
    mock_pub = mocker.MagicMock()
    mock_pub_init = mocker.patch("api.blueprints.drbCollection.Publication")
    mock_pub_init.return_value = mock_pub
    mock_db = mocker.MagicMock()
    collection = mocker.MagicMock(
        uuid="testUUID",
        title="Test Collection",
        creator="Test Creator",
        description="Test Description",
        editions=[mocker.MagicMock(id=1), mocker.MagicMock(id=2)],
        type="static",
    )
    mock_paging = mocker.patch.object(OPDSUtils, "addPagingOptions")
    mock_sort_con = mocker.patch("api.blueprints.drbCollection.constructSortMethod")
    mock_sort_con.return_value = (lambda x: str(x), False)

    with test_app.test_request_context("/collections/test"):
        test_opds_feed = constructOPDSFeed(collection, mock_db, sort="test")

        assert test_opds_feed == mock_feed
        mock_feed.addMetadata.assert_called_once_with(
            {
                "title": "Test Collection",
                "creator": "Test Creator",
                "description": "Test Description",
            }
        )
        mock_feed.addLink.assert_called_once_with(
            {
                "rel": "self",
                "href": "/collection/testUUID",
                "type": "application/opds+json",
            }
        )
        mock_feed.addPublications.assert_called_once()
        assert mock_pub.parseEditionToPublication.call_count == 2
        mock_pub.addLink.assert_has_calls(
            [
                mocker.call(
                    {
                        "rel": "alternate",
                        "href": "https://drb-qa.nypl.org/edition/1",
                        "type": "text/html",
                        "identifier": "readable",
                    }
                ),
                mocker.call(
                    {
                        "rel": "alternate",
                        "href": "https://drb-qa.nypl.org/edition/2",
                        "type": "text/html",
                        "identifier": "readable",
                    }
                ),
            ]
        )
        mock_sort_con.assert_called_once_with("test")
        mock_paging.assert_called_once_with(
            mock_feed, "/collection/testUUID", 2, page=1, perPage=10
        )


def test_construct_opds_feed_automatic_success(test_app, mocker):
    mock_feed = mocker.MagicMock()
    mock_feed_init = mocker.patch("api.blueprints.drbCollection.Feed")
    mock_feed_init.return_value = mock_feed
    mock_pub = mocker.MagicMock()
    mock_pub_init = mocker.patch("api.blueprints.drbCollection.Publication")
    mock_pub_init.return_value = mock_pub
    mock_db = mocker.MagicMock()
    collection = mocker.MagicMock(
        uuid="testUUID",
        title="Test Collection",
        creator="Test Creator",
        description="Test Description",
        type="automatic",
    )
    mock_db.fetchSingleCollection.return_value = collection
    mocker.patch(
        "api.blueprints.drbCollection.fetchAutomaticCollectionEditions",
        return_value=(
            mocker.sentinel.totalCount,
            [mocker.MagicMock(id=1), mocker.MagicMock(id=2)],
        ),
    )
    test_app.config["REDIS_CLIENT"] = "test_redis_client"
    mock_paging = mocker.patch.object(OPDSUtils, "addPagingOptions")

    with test_app.test_request_context("/collections/test"):
        test_opds_feed = constructOPDSFeed(collection, mock_db, sort="test")

        assert test_opds_feed == mock_feed
        mock_feed.addMetadata.assert_called_once_with(
            {
                "title": "Test Collection",
                "creator": "Test Creator",
                "description": "Test Description",
            }
        )
        mock_feed.addLink.assert_called_once_with(
            {
                "rel": "self",
                "href": "/collection/testUUID",
                "type": "application/opds+json",
            }
        )
        mock_feed.addPublications.assert_called_once()
        assert mock_pub.parseEditionToPublication.call_count == 2
        mock_pub.addLink.assert_has_calls(
            [
                mocker.call(
                    {
                        "rel": "alternate",
                        "href": "https://drb-qa.nypl.org/edition/1",
                        "type": "text/html",
                        "identifier": "readable",
                    }
                ),
                mocker.call(
                    {
                        "rel": "alternate",
                        "href": "https://drb-qa.nypl.org/edition/2",
                        "type": "text/html",
                        "identifier": "readable",
                    }
                ),
            ]
        )
        mock_paging.assert_called_once_with(
            mock_feed,
            "/collection/testUUID",
            mocker.sentinel.totalCount,
            page=1,
            perPage=10,
        )
