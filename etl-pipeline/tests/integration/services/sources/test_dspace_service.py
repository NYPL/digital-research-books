import pytest
import requests

from model import Source
from services import get_source_service, ServiceNotAvailableError


def test_get_records():
    dspace_service = get_source_service(Source.CLACSO.value)

    try:
        records = dspace_service.get_records(limit=5)

        for record in records:
            assert record is not None
    except (
        ServiceNotAvailableError,
        requests.exceptions.ReadTimeout,
        requests.exceptions.ConnectionError,
    ) as e:
        pytest.skip(f"Skipping due to external connection issue: {e}")
