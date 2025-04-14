from oauthlib.oauth2 import TokenExpiredError
import pytest

from managers import NYPLAPIManager
from tests.helper import TestHelpers


class TestNYPLAPIManager:
    @classmethod
    def setup_class(cls):
        TestHelpers.setEnvVars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clearEnvVars()

    @pytest.fixture
    def test_instance(self):
        return NYPLAPIManager()

    def test_initializer(self, test_instance):
        assert test_instance.client_id == 'test_api_client'
        assert test_instance.client_secret == 'test_api_secret'
        assert test_instance.token_url == 'test_api_token_url'
        assert test_instance.api_root == 'https://platform.nypl.org/api/v0.1'
        assert test_instance.token == None

    def test_generate_access_token(self, test_instance, mocker):
        mock_app_client = mocker.patch('managers.nypl_api.BackendApplicationClient')
        mock_app_client.return_value = 'testClient'
        mock_oauth_session = mocker.patch('managers.nypl_api.OAuth2Session')
        mock_oauth = mocker.MagicMock(name='OauthInstance')
        mock_oauth.fetch_token.return_value = 'testToken'
        mock_oauth_session.return_value = mock_oauth

        test_instance.generate_access_token()

        assert test_instance.token == 'testToken'
        mock_app_client.assert_called_once_with('test_api_client')
        mock_oauth_session.assert_called_once_with(client='testClient')
        mock_oauth.fetch_token.assert_called_once_with(
            token_url='test_api_token_url',
            client_id='test_api_client',
            client_secret='test_api_secret'
        )

    def test_create_client(self, test_instance, mocker):
        mock_oauth_session = mocker.patch('managers.nypl_api.OAuth2Session')
        mock_oauth_session.return_value = 'testClient'
        
        test_instance.create_client()

        assert test_instance.client == 'testClient'
        mock_oauth_session.assert_called_once_with('test_api_client', token=None)

    def test_query_api(self, test_instance, mocker):
        mock_client_create = mocker.patch.object(NYPLAPIManager, 'create_client')

        mock_resp = mocker.MagicMock()
        mock_resp.json.return_value = 'testAPIResponse'

        test_instance.client = mocker.MagicMock()
        test_instance.client.get.return_value = mock_resp

        test_response = test_instance.query_api('test/path/request')

        assert test_response == 'testAPIResponse'
        test_instance.client.get.assert_called_once_with(
            'https://platform.nypl.org/api/v0.1/test/path/request', timeout=15
        )
        mock_client_create.assert_not_called

    def test_query_api_token_expired(self, test_instance, mocker):
        mock_repeat_client = mocker.MagicMock()
        def reset_client():
            mock_resp = mocker.MagicMock()
            mock_resp.json.return_value = 'testAPIResponse'

            mock_repeat_client.get.return_value = mock_resp
            test_instance.client = mock_repeat_client

        mock_client_create = mocker.patch.object(NYPLAPIManager, 'create_client')
        mock_client_create.side_effect = reset_client

        mock_error_client = mocker.MagicMock()
        mock_error_client.get.side_effect = TokenExpiredError
        test_instance.client = mock_error_client

        mock_token_generate = mocker.patch.object(NYPLAPIManager, 'generate_access_token')

        test_response = test_instance.query_api('test/path/request')

        assert test_response == 'testAPIResponse'
        mock_error_client.get.assert_called_once_with(
            'https://platform.nypl.org/api/v0.1/test/path/request', timeout=15
        )
        mock_repeat_client.get.assert_called_once_with(
            'https://platform.nypl.org/api/v0.1/test/path/request', timeout=15
        )
        mock_token_generate.assert_called_once
        mock_client_create.assert_called_once

    def test_query_api_timeout(self, test_instance, mocker):
        test_instance.client = mocker.MagicMock()
        test_instance.client.get.side_effect = TimeoutError
        
        assert test_instance.query_api('test/path/request') == {}
        