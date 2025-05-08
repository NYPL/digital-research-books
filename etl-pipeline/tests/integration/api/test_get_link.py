import os
import pytest
import requests

from .utils import assert_response_status


@pytest.mark.parametrize(
    "endpoint, expected_status",
    [
        ("/links/{link_id}", 200),
        ("/links/00000000-0000-0000-0000-000000000000", 400),
        ("/links/invalid_id_format", 400),
        ("/links/", 404),
        ("/links/%$@!*", 400),
    ],
)
def test_get_link(endpoint, expected_status, test_link_id):
    url = os.getenv("DRB_API_URL") + endpoint.format(link_id=test_link_id)
    response = requests.get(url)

    assert response.status_code is not None
    assert_response_status(url, response, expected_status)

    if expected_status == 200:
        response_json = response.json()
        assert response_json is not None
