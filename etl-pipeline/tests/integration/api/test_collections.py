import os
import pytest
import requests

from .utils import assert_response_status


@pytest.mark.parametrize(
    "endpoint, expected_status",
    [
        ("/collections/{collection_id}", 200),
        ("/collections/00000000-0000-0000-0000-000000000000", 404),
        ("/collections/invalid_id_format", 400),
        ("/collections/", 404),
        ("/collections/%$@!*", 400),
    ],
)
def test_get_collection(endpoint, expected_status, test_collection_id):
    url = os.getenv("DRB_API_URL") + endpoint.format(collection_id=test_collection_id)
    response = requests.get(url)

    assert response.status_code is not None
    assert_response_status(url, response, expected_status)

    if expected_status == 200:
        response_json = response.json()
        assert response_json is not None


def test_get_collections():
    url = os.getenv("DRB_API_URL") + "/collections?sort=title"
    response = requests.get(url)

    assert response.status_code is not None
    assert_response_status(url, response, 200)

    response_json = response.json()

    assert response_json is not None
