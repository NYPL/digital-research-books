import pytest
import os
import requests

from .utils import assert_response_status


@pytest.mark.parametrize("endpoint, expected_status", [
    ("/editions/{edition_id}", 200),
    ("/editions/00000000-0000-0000-0000-000000000000", 400),
    ("/editions/invalid_id_format", 400),
    ("/editions/", 404),
    ("/editions/%$@!*", 400)
])
def test_get_edition(endpoint, expected_status, test_edition_id): 
    url = os.getenv("DRB_API_URL") + endpoint.format(edition_id=test_edition_id)
    response = requests.get(url)

    assert response.status_code is not None
    assert_response_status(url, response, expected_status)
    print (assert_response_status)

    if expected_status == 200:
        response_json = response.json()
        assert response_json is not None
