import pytest
import requests

from ..utils import assert_response_status, get_vra_auth_headers
from utils.common import require_env

ENDPOINT_PATH = "/chat"

TOP_LEVEL_FIELDS = {
    "status": int,
    "timestamp": str,
    "responseType": str,
    "data": dict,
}

TEST_CASES = [  # Defined as tuples of (conversation_type, message, edition_id)
    ("catalogSearch", "Find something on fire.", None),
    ("contentSearch", "What are the main topics discussed in this text?", 15649870),
]


@pytest.mark.parametrize("conversation_type, message, edition_id", TEST_CASES)
def test_chat(conversation_type, message, edition_id, vra_test_user):
    url = require_env("DRB_API_URL") + ENDPOINT_PATH
    payload = {
        "conversationType": conversation_type,
        "message": message,
    }

    if edition_id is not None:
        payload["editionId"] = edition_id

    response = requests.post(
        url,
        json=payload,
        headers=get_vra_auth_headers(),
        timeout=90,  # 30s faster than pytest timeout to catch API timeouts explicitly
    )

    # Verify HTTP status code is returned and is 200 OK
    assert response.status_code is not None
    assert_response_status(url, response, 200)

    # Verify response format is JSON
    try:
        response_json = response.json()
    except ValueError:
        response_json = None
        pytest.fail("Response is not valid JSON")
    assert response_json is not None

    # Verify expected top-level fields are present in the response
    for field in TOP_LEVEL_FIELDS.keys():
        assert field in response_json, f"Missing expected top-level field: {field}"

    # Verify top-level fields are of the expected type
    for field, expected_type in TOP_LEVEL_FIELDS.items():
        assert isinstance(response_json.get(field), expected_type), (
            f"Expected {field} to be of type {expected_type.__name__}"
        )
