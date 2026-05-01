import pytest


from tests.helper import TestHelpers
from processes import APIProcess
from unittest.mock import patch


class TestAPIProcess:
    @classmethod
    def setup_class(cls):
        TestHelpers.setEnvVars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clearEnvVars()

    @pytest.fixture
    def api_process(self):
        with (
            patch("processes.api.ElasticsearchManager"),
            patch("processes.api.RedisManager"),
        ):
            return APIProcess()

    def test_run_process_success(self, api_process: APIProcess):
        with patch("processes.api.API") as MockAPI:
            mock_api = MockAPI.return_value

            api_process.run()

            api_process.redis_manager.create_client.assert_called_once()
            api_process.elastic_search_manager.create_elastic_connection.assert_called_once()
            mock_api.create_error_responses.assert_called_once()
            mock_api.run.assert_called_once()

    def test_run_process_failure(self, api_process: APIProcess):
        api_process.elastic_search_manager.create_elastic_connection.side_effect = (
            Exception("Connection error")
        )

        with pytest.raises(Exception):
            api_process.run()
