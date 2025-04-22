import pytest

from services import PublisherBacklistService

class TestPublisherBacklistService:
    @pytest.fixture
    def test_instance(self):
        return PublisherBacklistService()

    def test_get_records(self, test_instance: PublisherBacklistService):
        records = test_instance.get_records(limit=5)

        for record in records:
            assert record is not None
