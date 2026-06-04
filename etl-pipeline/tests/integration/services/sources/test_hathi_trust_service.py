from datetime import datetime, timedelta

import pytest
import requests
from services import HathiTrustService


def test_get_records():
    hathi_trust_service = HathiTrustService()

    try:
        records = hathi_trust_service.get_records(
            limit=5, start_timestamp=datetime.now() - timedelta(days=7)
        )

        for record in records:
            assert record is not None
    except requests.exceptions.RequestException as e:
        pytest.skip(f"HathiTrust API unavailable: {e}")
