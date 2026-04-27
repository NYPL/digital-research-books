from datetime import datetime, timezone, timedelta
import pytest
import os
from services import NYPLBibService


class TestNYPLBibService:
    @pytest.fixture
    def test_instance(self):
        return NYPLBibService()

    # NOTE: This test depends on NYPL_BIB_HOST env var which is in the NYPL AWS \
    # private subnet (i.e. VPN required for at home execution).
    def test_get_records(self, test_instance: NYPLBibService):
        records = test_instance.get_records(
            start_timestamp=datetime.now(timezone.utc).replace(tzinfo=None)
            - timedelta(hours=24),
            limit=100,
        )

        for record in records:
            assert record is not None
