import os
import pytest

from api.app import API


class TestAPI:
    @pytest.fixture
    def test_api(self, mocker):
        class MockTestAPI(API):
            def __init__(self, db_engine, redis_client):
                self.app = mocker.MagicMock()

        return MockTestAPI("testDBEngine", "testRedisClient")

    def test_run_local(self, test_api):
        os.environ["STAGE"] = "development"
        os.environ["DRB_API_HOST"] = "127.0.0.1"

        test_api.run()

        test_api.app.run.assert_called_once_with(host="127.0.0.1", port=5050)

    def test_run_production(self, test_api, mocker):
        os.environ.pop("STAGE", None)
        mock_serve = mocker.patch("api.app.serve")

        test_api.run()

        mock_serve.assert_called_once_with(test_api.app, host="0.0.0.0", port=80)
