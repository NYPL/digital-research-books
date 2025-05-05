from datetime import datetime, timezone, timedelta
import pytest
import os
from services import NYPLBibService


class TestNYPLBibService:
    @pytest.fixture
    def test_instance(self):
        return NYPLBibService()

    @pytest.mark.skipif(os.getenv('IS_CI') == 'true' or os.getenv('ENVIRONMENT') == 'qa', reason="Skipping in CI environment")    
    def test_get_records(self, test_instance: NYPLBibService):
        records = test_instance.get_records(
            start_timestamp=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24), 
            limit=100
        )

        for record in records:
            assert record is not None
