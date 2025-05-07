import os
import pytest
import requests

from .utils import assert_response_status


@pytest.mark.parametrize(
    "endpoint, expected_status",
    [
        ("/works/{work_id}", 200),
        ("/works/00000000-0000-0000-0000-000000000000", 404),
        ("/works/invalid_id_format", 400),
        ("/works/", 404),
        ("/works/%$@!*", 400),
    ],
)
def test_get_work(endpoint, expected_status, test_work_id):
    url = os.getenv("DRB_API_URL") + endpoint.format(work_id=test_work_id)
    response = requests.get(url)

    assert response.status_code is not None
    assert_response_status(url, response, expected_status)

    if expected_status == 200:
        response_json = response.json()
        assert response_json is not None
