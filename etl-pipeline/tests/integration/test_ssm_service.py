import pytest

from services.ssm_service import SSMService


class TestSSMService:
    @pytest.fixture
    def test_instance(self):
        return SSMService()

    def test_get_parameter(self, test_instance):
        TEST_PARAM = "nypl-api/public-key"
        value = test_instance.get_parameter(TEST_PARAM)

        assert value != None
