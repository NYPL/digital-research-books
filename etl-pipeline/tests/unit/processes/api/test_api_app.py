import os
import pytest

from api.app import FlaskAPI


class TestFlaskAPI:
    @pytest.fixture
    def testInstance(self, mocker):
        class MockFlaskAPI(FlaskAPI):
            def __init__(self, client, dbEngine):
                self.app = mocker.MagicMock()

        return MockFlaskAPI("testClient", "testEngine")

    def test_run_local(self, testInstance):
        os.environ["ENVIRONMENT"] = "local"
        os.environ["STAGE"] = "development"

        testInstance.run()

        testInstance.app.assert_called_once

    def test_run_production(self, testInstance, mocker):
        os.environ["ENVIRONMENT"] = "production"
        del os.environ["STAGE"]
        mockServe = mocker.patch("api.app.serve")

        testInstance.run()

        mockServe.assert_called_once_with(testInstance.app, host="0.0.0.0", port=80)
