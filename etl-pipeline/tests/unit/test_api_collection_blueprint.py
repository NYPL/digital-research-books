from flask import Flask
import os
import pytest
from sqlalchemy.orm.exc import NoResultFound

from api.blueprints.drbCollection import (
    collectionCreate, get_collection, collectionReplace, collectionUpdate, collectionDelete,
    collectionDeleteWorkEdition, get_collections, constructSortMethod, constructOPDSFeed, validateToken
)
from api.utils import APIUtils
from api.opdsUtils import OPDSUtils


class TestCollectionBlueprint:
    @pytest.fixture
    def mock_utils(self, mocker):
        return mocker.patch.multiple(
            APIUtils,
            formatResponseObject=mocker.DEFAULT,
            formatOPDS2Object=mocker.DEFAULT,
            validatePassword=mocker.DEFAULT
        )

    @pytest.fixture
    def test_app(self):
        flask_app = Flask('test')
        flask_app.config['DB_CLIENT'] = 'testDBClient'

        return flask_app

    def test_collection_replace_success(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        collection = mocker.MagicMock(uuid="testUUID")
        mock_db.fetchSingleCollection.return_value = collection
        mock_feed_contstruct = mocker.patch(
            'api.blueprints.drbCollection.constructOPDSFeed'
        )
        mock_feed_contstruct.return_value = 'testOPDS2Feed'

        test_updated_collection = {
            'title': 'Updated Test Collection',
            'creator': 'Updated Test Creator',
            'description': 'Updated Test Description',
            'editionIDs': ['ed11', 'ed22']
        }

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        mock_utils['formatOPDS2Object'].return_value = 'testOPDS2Response'

        with test_app.test_request_context(
            '/replace/testUUID',
            json=test_updated_collection,
            headers={'Authorization': 'Basic testAuth'}
        ):
            test_api_response = collectionReplace('testUUID')

            assert test_api_response == 'testOPDS2Response'

            assert mock_db_client.call_count == 2
            assert mock_db.createSession.call_count == 2
            assert mock_db.session.execute.call_count == 1
            mock_db.fetchSingleCollection.assert_called_once_with('testUUID')
            mock_db.session.commit.assert_called_once()

            mock_feed_contstruct.assert_called_once_with(
                collection, mock_db
            )

            mock_base_64.assert_called_once_with(b'testAuth')

            mock_utils['formatOPDS2Object'].assert_called_once_with(
                201, 'testOPDS2Feed'
            )

    def test_collection_replace_fail(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        mock_db.fetchSingleCollection\
            .return_value = mocker.MagicMock(uuid='testUUID')

        test_fail_collection = {
            'title': 'Updated Test Collection',
            'creator': 'Updated Test Creator'
        }

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        with test_app.test_request_context(
            '/replace/testUUID',
            json=test_fail_collection,
            headers={'Authorization': 'Basic testAuth'}
        ):

            test_api_response = collectionReplace('testUUID')

            assert test_api_response == 'testErrorResponse'

            mock_utils['formatResponseObject'].assert_called_once_with(
                400,
                'createCollection',
                {
                    'message':
                    'title, creator and description fields are required'
                    ', with one of workUUIDs or editionIDs to create a'
                    ' collection'

                }
            )


    def test_collection_update_success(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        collection = mocker.MagicMock(uuid="testUUID")
        mock_db.fetchSingleCollection.return_value = collection

        mock_feed_construct = mocker.patch(
            'api.blueprints.drbCollection.constructOPDSFeed'
        )
        mock_feed_construct.return_value = 'testOPDS2Feed'

        mock_utils['formatOPDS2Object'].return_value = 'testOPDS2Response'

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        with test_app.test_request_context(
            '/update/testUUID?title=newTitle',
            headers={'Authorization': 'Basic testAuth'}
        ):
            test_api_response = collectionUpdate('testUUID')

            assert test_api_response == 'testOPDS2Response'

            assert mock_db_client.call_count == 2
            assert mock_db.createSession.call_count == 2
            mock_db.fetchSingleCollection.assert_called_once_with('testUUID')
            mock_db.session.commit.assert_called_once()

            mock_feed_construct.assert_called_once_with(
                collection, mock_db
            )

            mock_utils['formatOPDS2Object'].assert_called_once_with(
                200, 'testOPDS2Feed'
            )

    def test_collection_update_error(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        mock_db.fetchSingleCollection\
            .return_value = mocker.MagicMock(uuid='testUUID')

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        with test_app.test_request_context(
            '/update/testUUID',
            headers={'Authorization': 'Basic testAuth'}
        ):

            test_api_response = collectionUpdate('testUUID')

            assert test_api_response == 'testErrorResponse'

            mock_utils['formatResponseObject'].assert_called_once_with(
                400,
                'updateCollection',
                {
                    'message':
                    'At least one of these fields(title, creator, description, etc.) are required'
                }
            )

    def test_static_collection_createsuccess(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        collection = mocker.MagicMock(uuid='testUUID')
        mock_db.createStaticCollection.return_value = collection

        mock_feed_construct = mocker.patch(
            'api.blueprints.drbCollection.constructOPDSFeed'
        )
        mock_feed_construct.return_value = 'testOPDS2Feed'

        test_request_body = {
            'title': 'Test Collection',
            'creator': 'Test Creator',
            'description': 'Test Description',
            'workUUIDs': ['uuid1', 'uuid2'],
            'editionIDs': ['ed1', 'ed2', 'ed3']
        }

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        mock_utils['formatOPDS2Object'].return_value = 'testOPDS2Response'

        with test_app.test_request_context(
            '/',
            json=test_request_body,
            headers={'Authorization': 'Basic testAuth'}
        ):
            test_api_response = collectionCreate()

            assert test_api_response == 'testOPDS2Response'

            assert mock_db_client.call_count == 2
            assert mock_db.createSession.call_count == 2
            mock_db.createStaticCollection.assert_called_once_with(
                'Test Collection', 'Test Creator', 'Test Description',
                'testUser', workUUIDs=['uuid1', 'uuid2'],
                editionIDs=['ed1', 'ed2', 'ed3']
            )
            mock_db.session.commit.assert_called_once()

            mock_feed_construct.assert_called_once_with(collection, mock_db)

            mock_base_64.assert_called_once_with(b'testAuth')

            mock_utils['formatOPDS2Object'].assert_called_once_with(
                201, 'testOPDS2Feed'
            )

    def test_automatic_collection_create_success(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        mock_db.createAutomaticCollection.return_value = mocker.MagicMock(uuid='testUUID')

        mock_feed_construct = mocker.patch(
            'api.blueprints.drbCollection.constructOPDSFeed'
        )
        mock_feed_construct.return_value = 'testOPDS2Feed'

        test_request_body = {
            'title': 'Test Collection',
            'creator': 'Test Creator',
            'description': 'Test Description',
            'autoDef': {
                'sortField': 'date',
                'sortDirection': 'ASC',
                'keywordQuery': 'bikes',
            },
        }

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        mock_utils['formatOPDS2Object'].return_value = 'testOPDS2Response'

        with test_app.test_request_context(
            '/',
            json=test_request_body,
            headers={'Authorization': 'Basic testAuth'}
        ):
            test_api_response = collectionCreate()

            assert test_api_response == 'testOPDS2Response'

            mock_db.createAutomaticCollection.assert_called_once_with(
                'Test Collection', 'Test Creator', 'Test Description',
                owner='testUser', sortField='date', sortDirection='ASC',
                limit=None, keywordQuery='bikes', authorQuery=None, titleQuery=None,
                subjectQuery=None,
            )
            mock_db.session.commit.assert_called_once()

            mock_feed_construct.assert_called_once_with(
                mock_db.createAutomaticCollection.return_value, mock_db,
            )

            mock_base_64.assert_called_once_with(b'testAuth')

            mock_utils['formatOPDS2Object'].assert_called_once_with(
                201, 'testOPDS2Feed'
            )

    def test_automatic_collection_create_invalid(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        test_request_body = {
            'title': 'Test Collection',
            'creator': 'Test Creator',
            'description': 'Test Description',
            'autoDef': {
                'sortField': 'bad_sort_field',
                'keywordQuery': 'bikes',
            },
        }

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        with test_app.test_request_context(
            '/',
            json=test_request_body,
            headers={'Authorization': 'Basic testAuth'}
        ):
            test_api_response = collectionCreate()

            assert test_api_response == 'testErrorResponse'

            mock_db.createAutomaticCollection.assert_not_called()
            mock_utils['formatResponseObject'].assert_called_once_with(
                400, "createCollection",
                {'message': "Invalid sort field bad_sort_field"},
            )

            mock_base_64.assert_called_once_with(b'testAuth')

    def test_collection_create_invalid(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        test_request_body = {
            'title': 'Test Collection',
            'creator': 'Test Creator',
            'description': 'Test Description',
            'editionIDs': [1, 2, 3],
            'autoDef': {
                'sortField': 'bad_sort_field',
                'keywordQuery': 'bikes',
            },
        }

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        with test_app.test_request_context(
            '/',
            json=test_request_body,
            headers={'Authorization': 'Basic testAuth'}
        ):
            test_api_response = collectionCreate()

            assert test_api_response == 'testErrorResponse'

            mock_db.createAutomaticCollection.assert_not_called()
            mock_utils['formatResponseObject'].assert_called_once_with(
                400, "createCollection",
                {
                    'message': (
                        "Cannot create a collection with both an automatic collection "
                        "definition and editionIDs or workUUIDs"
                    ),
                },
            )

            mock_base_64.assert_called_once_with(b'testAuth')

    def test_collection_create_error(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        test_request_body = {
            'creator': 'Test Creator',
            'description': 'Test Description',
            'workUUIDs': ['uuid1', 'uuid2'],
            'editionIDs': ['ed1', 'ed2', 'ed3']
        }

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        with test_app.test_request_context(
            '/',
            json=test_request_body,
            headers={'Authorization': 'Basic testAuth'}
        ):
            test_api_response = collectionCreate()

            assert test_api_response == 'testErrorResponse'

            mock_utils['formatResponseObject'].assert_called_once_with(
                400,
                'createCollection',
                {
                    'message': 'title, creator and description fields are required',
                }
            )

    def test_get_collection_success(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        collection = mocker.MagicMock(uuid="d902fd44-7cbe-4401-b50c-5b1bda8b1059")
        mock_db.fetchSingleCollection.return_value = collection

        mock_feed_construct = mocker.patch(
            'api.blueprints.drbCollection.constructOPDSFeed'
        )
        mock_feed_construct.return_value = 'testOPDS2Feed'

        mock_utils['formatOPDS2Object'].return_value = 'testOPDS2Response'

        with test_app.test_request_context('/?sort=title&page=3'):
            test_api_response = get_collection('d902fd44-7cbe-4401-b50c-5b1bda8b1059')

            assert test_api_response == 'testOPDS2Response'

            mock_db.createSession.assert_called_once()

            mock_feed_construct.assert_called_once_with(
                collection, mock_db, sort='title', page=3, perPage=10
            )

            mock_utils['formatOPDS2Object'].assert_called_once_with(
                200, 'testOPDS2Feed'
            )

    def test_get_collection_not_found(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchSingleCollection.side_effect = NoResultFound

        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        with test_app.test_request_context('/?sort=title&page=3'):
            test_api_response = get_collection('d902fd44-7cbe-4401-b50c-5b1bda8b1059')

            assert test_api_response == 'testErrorResponse'

            mock_db.createSession.assert_called_once()

            mock_utils['formatResponseObject'].assert_called_once_with(
                404, 
                'fetchCollection',
                {'message': 'No collection found with id d902fd44-7cbe-4401-b50c-5b1bda8b1059'}
            )

    def test_get_collection_error(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchSingleCollection.side_effect = Exception('Database error')

        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        with test_app.test_request_context('/?sort=title&page=3'):
            test_api_response = get_collection('d902fd44-7cbe-4401-b50c-5b1bda8b1059')

            assert test_api_response == 'testErrorResponse'

            mock_db.createSession.assert_called_once()

            mock_utils['formatResponseObject'].assert_called_once_with(
                500, 
                'fetchCollection',
                {'message': 'Unable to get collection with id d902fd44-7cbe-4401-b50c-5b1bda8b1059'}
            )

    def test_get_collection_invalid_id(self, test_app, mock_utils):
        mock_utils['formatResponseObject'].return_value = '400response'

        with test_app.test_request_context('/?sort=title&page=3'):
            test_api_response = get_collection('testUUID')

            assert test_api_response == '400response'

            mock_utils['formatResponseObject'].assert_called_once_with(
                400, 
                'fetchCollection',
                {'message': 'Collection id testUUID is invalid'}
            )

    def test_collection_delete_success(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        mock_db.deleteCollection.return_value = 1

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        with test_app.test_request_context(
            '/', headers={'Authorization': 'Bearer testToken'}
        ):
            test_api_response = collectionDelete('testUUID')

            assert test_api_response[0].status_code == 200
            assert test_api_response[0].json == {'message': 'Deleted testUUID'}

            assert mock_db.createSession.call_count == 2
            mock_db.deleteCollection.assert_called_once_with(
                'testUUID'
            )
            mock_db.session.commit.assert_called_once()

    def test_collection_delete_error(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        mock_db.deleteCollection.return_value = 0

        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        with test_app.test_request_context(
            '/', headers={'Authorization': 'Bearer testToken'}
        ):
            test_api_response = collectionDelete('testUUID')

            assert test_api_response == 'testErrorResponse'

            assert mock_db.createSession.call_count == 2
            mock_db.deleteCollection.assert_called_once_with(
                'testUUID'
            )

            mock_utils['formatResponseObject'].assert_called_once_with(
                404,
                'deleteCollection',
                {'message': 'No collection with UUID testUUID exists'}
            )

    def test_collection_delete_work_edition_success(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        collection = mocker.MagicMock(uuid="testUUID")
        mock_db.fetchSingleCollection.return_value = collection

        mock_feed_construct = mocker.patch(
            'api.blueprints.drbCollection.constructOPDSFeed'
        )
        mock_feed_construct.return_value = 'testOPDS2Feed'

        mockRemoveEdition = mocker.patch(
            'api.blueprints.drbCollection.removeWorkEditionsFromCollection'
        )

        mock_utils['formatOPDS2Object'].return_value = 'testOPDS2Response'

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        with test_app.test_request_context(
            '/delete/testUUID?editionIDs=testID',
            headers={'Authorization': 'Basic testAuth'}
        ):
            test_api_response = collectionDeleteWorkEdition('testUUID')

            assert test_api_response == 'testOPDS2Response'

            assert mock_db_client.call_count == 2
            assert mock_db.createSession.call_count == 2
            assert mockRemoveEdition.call_count == 1
            mock_db.fetchSingleCollection.assert_called_once_with('testUUID')
            mock_db.session.commit.assert_called_once()

            mock_feed_construct.assert_called_once_with(collection, mock_db)

            mock_utils['formatOPDS2Object'].assert_called_once_with(
                200, 'testOPDS2Feed'
            )

    def test_collection_delete_work_edition_error(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        mock_db.fetchSingleCollection\
            .return_value = mocker.MagicMock(uuid='testUUID')

        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        mocker.patch.dict(os.environ, {'NYPL_API_CLIENT_PUBLIC_KEY': 'test'})

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        with test_app.test_request_context(
            '/delete/testUUID',
            headers={'Authorization': 'Basic testAuth'}
        ):
            test_api_response = collectionDeleteWorkEdition('testUUID')

            assert test_api_response == 'testErrorResponse'

            assert mock_db_client.call_count == 1
            assert mock_db.createSession.call_count == 1

            mock_utils['formatResponseObject'].assert_called_once_with(
                400, 'deleteCollectionWorkEdition', {
                    'message':
                        'At least one of these fields(editionIDs & workUUIDs) are required'
                }
            )

    def test_get_collections_success(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        collection1 = mocker.MagicMock(uuid='uuid1')
        collection2 = mocker.MagicMock(uuid='uuid2')
        mock_db.fetchCollections.return_value = [collection1, collection2]

        mock_feed = mocker.MagicMock()
        mock_feed_init = mocker.patch('api.blueprints.drbCollection.Feed')
        mock_feed_init.return_value = mock_feed

        mock_paging = mocker.patch.object(OPDSUtils, 'addPagingOptions')

        mockConstruct = mocker.patch(
            'api.blueprints.drbCollection.constructOPDSFeed'
        )
        mockConstruct.side_effect = ['group1', 'group2']

        mock_utils['formatOPDS2Object'].return_value = 'testOPDSResponse'

        with test_app.test_request_context('/list'):
            test_response = get_collections()

            assert test_response == 'testOPDSResponse'

            mock_db.createSession.assert_called_once()
            mock_db.fetchCollections.assert_called_once_with(
                sort='title', page=1, perPage=10
            )

            mock_feed_init.assert_called_once()
            mock_feed.addMetadata.assert_called_once_with(
                {'title': 'Digital Research Books Collections'}
            )
            mock_feed.addLink.assert_called_once_with({
                'rel': 'self',
                'href': '/list',
                'type': 'application/opds+json'
            })

            mock_paging.assert_called_once_with(
                mock_feed, '/list?', 2, page=1, perPage=10
            )

            mockConstruct.assert_has_calls([
                mocker.call(
                    collection1, mock_db, perPage=5, path='/collection/uuid1'
                ),
                mocker.call(
                    collection2, mock_db, perPage=5, path='/collection/uuid2'
                )
            ])

            mock_feed.addGroup.assert_has_calls([
                mocker.call('group1'), mocker.call('group2')
            ])

            mock_utils['formatOPDS2Object'].assert_called_once_with(
                200, mock_feed
            )

    def test_get_collections_error(self, test_app, mock_utils, mocker):
        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchCollections.side_effect = Exception('Database error')

        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        with test_app.test_request_context('/list'):
            test_response = get_collections()

            assert test_response == 'testErrorResponse'

            mock_db.createSession.assert_called_once()
            mock_db.fetchCollections.assert_called_once_with(
                sort='title', page=1, perPage=10
            )

            mock_utils['formatResponseObject'].assert_called_once_with(
                500, 
                'collectionList',
                { 'message': 'Unable to get collections' }
            )

    def test_get_collections_sort_error(self, test_app, mock_utils):
        mock_utils['formatResponseObject'].return_value = 'testErrorResponse'

        with test_app.test_request_context('/list?sort=error'):
            test_response = get_collections()

            assert test_response == 'testErrorResponse'

            mock_utils['formatResponseObject'].assert_called_once_with(
                400, 
                'collectionList',
                { 'message': 'Sort fields are invalid' }
            )

    def test_construct_sort_method_string(self, mocker):
        sort_method, reversed = constructSortMethod('test')

        test_sorts = [
            mocker.MagicMock(metadata=mocker.MagicMock(id=1, test='b')),
            mocker.MagicMock(metadata=mocker.MagicMock(id=2, test='A')),
            mocker.MagicMock(metadata=mocker.MagicMock(id=3, test='c')),
        ]

        sorted_list = sorted(test_sorts, key=sort_method, reverse=reversed)

        assert [x.metadata.id for x in sorted_list] == [2, 1, 3]

    def test_construct_sort_methodint_reversed(self, mocker):
        sort_method, reversed = constructSortMethod('test:desc')

        test_sorts = [
            mocker.MagicMock(metadata=mocker.MagicMock(id=1, test=3)),
            mocker.MagicMock(metadata=mocker.MagicMock(id=2, test=1)),
            mocker.MagicMock(metadata=mocker.MagicMock(id=3, test=2)),
        ]

        sorted_list = sorted(test_sorts, key=sort_method, reverse=reversed)

        assert [x.metadata.id for x in sorted_list] == [1, 3, 2]

    def test_construct_opds_feed_success(self, test_app, mock_utils, mocker):
        mock_feed = mocker.MagicMock()
        mock_feed_init = mocker.patch('api.blueprints.drbCollection.Feed')
        mock_feed_init.return_value = mock_feed

        mock_pub = mocker.MagicMock()
        mock_pub_init = mocker.patch('api.blueprints.drbCollection.Publication')
        mock_pub_init.return_value = mock_pub

        mock_db = mocker.MagicMock()
        collection = mocker.MagicMock(
            uuid="testUUID",
            title='Test Collection',
            creator='Test Creator',
            description='Test Description',
            editions=[
                mocker.MagicMock(id=1), mocker.MagicMock(id=2)
            ],
            type="static",
        )

        mocker.patch.dict(os.environ, {'ENVIRONMENT': 'test'})

        mock_paging = mocker.patch.object(OPDSUtils, 'addPagingOptions')

        mock_sort_con = mocker.patch(
            'api.blueprints.drbCollection.constructSortMethod'
        )
        mock_sort_con.return_value = (lambda x: str(x), False)

        with test_app.test_request_context('/collections/test'):
            test_opds_feed = constructOPDSFeed(collection, mock_db, sort='test')

            assert test_opds_feed == mock_feed

            mock_feed.addMetadata.assert_called_once_with({
                'title': 'Test Collection',
                'creator': 'Test Creator',
                'description': 'Test Description'
            })
            mock_feed.addLink.assert_called_once_with({
                'rel': 'self',
                'href': '/collection/testUUID',
                'type': 'application/opds+json'
            })
            mock_feed.addPublications.assert_called_once()

            assert mock_pub.parseEditionToPublication.call_count == 2
            mock_pub.addLink.assert_has_calls([
                mocker.call({
                    'rel': 'alternate',
                    'href': 'https://drb-qa.nypl.org/edition/1',
                    'type': 'text/html',
                    'identifier': 'readable'
                }),
                mocker.call({
                    'rel': 'alternate',
                    'href': 'https://drb-qa.nypl.org/edition/2',
                    'type': 'text/html',
                    'identifier': 'readable'
                })
            ])

            mock_sort_con.assert_called_once_with('test')

            mock_paging.assert_called_once_with(
                mock_feed, '/collection/testUUID', 2, page=1, perPage=10
            )

    def test_construct_opds_Feed_success_auto_collection(self, test_app, mock_utils, mocker):
        mock_feed = mocker.MagicMock()
        mock_feed_init = mocker.patch('api.blueprints.drbCollection.Feed')
        mock_feed_init.return_value = mock_feed

        mock_pub = mocker.MagicMock()
        mock_pub_init = mocker.patch('api.blueprints.drbCollection.Publication')
        mock_pub_init.return_value = mock_pub

        mock_db = mocker.MagicMock()
        collection = mocker.MagicMock(
            uuid='testUUID',
            title='Test Collection',
            creator='Test Creator',
            description='Test Description',
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

        mocker.patch.dict(
            os.environ,
            {'ENVIRONMENT': 'test', 'ELASTICSEARCH_INDEX': 'test_es_index'},
        )
        test_app.config['REDIS_CLIENT'] = 'test_redis_client'

        mock_paging = mocker.patch.object(OPDSUtils, 'addPagingOptions')

        with test_app.test_request_context('/collections/test'):
            test_opds_feed = constructOPDSFeed(collection, mock_db, sort='test')

            assert test_opds_feed == mock_feed

            mock_feed.addMetadata.assert_called_once_with({
                'title': 'Test Collection',
                'creator': 'Test Creator',
                'description': 'Test Description'
            })
            mock_feed.addLink.assert_called_once_with({
                'rel': 'self',
                'href': '/collection/testUUID',
                'type': 'application/opds+json'
            })
            mock_feed.addPublications.assert_called_once()

            assert mock_pub.parseEditionToPublication.call_count == 2
            mock_pub.addLink.assert_has_calls([
                mocker.call({
                    'rel': 'alternate',
                    'href': 'https://drb-qa.nypl.org/edition/1',
                    'type': 'text/html',
                    'identifier': 'readable'
                }),
                mocker.call({
                    'rel': 'alternate',
                    'href': 'https://drb-qa.nypl.org/edition/2',
                    'type': 'text/html',
                    'identifier': 'readable'
                })
            ])

            mock_paging.assert_called_once_with(
                mock_feed, '/collection/testUUID', mocker.sentinel.totalCount, page=1, perPage=10
            )

    def test_validate_token_success(self, test_app, mock_utils, mocker):
        mock_func = mocker.MagicMock()

        decorated_function = validateToken(mock_func)

        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = True

        with test_app.test_request_context(
            '/', headers={'Authorization': 'Basic testAuth'}
        ):
            decorated_function()

            mock_base_64.assert_called_once_with(b'testAuth')

            mock_func.assert_called_once_with(user='testUser')

    def test_validate_token_error_no_header(self, test_app, mock_utils, mocker):
        mock_func = mocker.MagicMock()

        decorated_function = validateToken(mock_func)

        mock_utils['formatResponseObject'].return_value = 'testError'

        with test_app.test_request_context('/'):
            test_response = decorated_function()

            assert test_response == 'testError'

            mock_utils['formatResponseObject'].assert_called_once_with(
                403, 'authResponse',
                {'message': 'user/password not provided'}
            )

    def test_validate_token_error_auth(self, test_app, mock_utils, mocker):
        mock_func = mocker.MagicMock()

        decorated_function = validateToken(mock_func)

        mock_db = mocker.MagicMock(session=mocker.MagicMock())
        mock_db_client = mocker.patch('api.blueprints.drbCollection.DBClient')
        mock_db_client.return_value = mock_db

        mock_db.fetchUser.return_value = mocker.MagicMock(
            user='testUser', password='testPswd', salt='testSalt'
        )

        mock_base_64 = mocker.patch('api.blueprints.drbCollection.b64decode')
        mock_base_64.return_value = b'testUser:testPswd'

        mock_utils['validatePassword'].return_value = False

        mock_utils['formatResponseObject'].return_value = 'testError'

        with test_app.test_request_context(
            '/', headers={'Authorization': 'Basic testAuth'}
        ):
            test_response = decorated_function()

            assert test_response == 'testError'

            mock_utils['formatResponseObject'].assert_called_once_with(
                401, 'authResponse',
                {'message': 'invalid user/password'}
            )
