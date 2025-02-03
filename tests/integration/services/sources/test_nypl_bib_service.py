from datetime import datetime, timezone, timedelta
import pytest
import os
from services import NYPLBibService


class TestNYPLBibService:
    @pytest.fixture
    def test_instance(self):
        return NYPLBibService()

    @pytest.mark.skipif(os.getenv('IS_CI') == 'true', reason="Skipping in CI environment")    
    def test_get_records(self, test_instance: NYPLBibService):
        records = test_instance.get_records(
            start_timestamp=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24), 
            limit=100
        )

        assert records is not None
