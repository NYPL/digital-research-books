import os
import pytest
import requests
from sqlalchemy import text

from ..utils import assert_response_status, get_vra_auth_headers
from utils.common import require_env
from api.db import get_engine
from api.session_jwt import sign_session

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
def test_chat(conversation_type, message, edition_id, vra_test_user, test_session_id):
    url = require_env("DRB_API_URL") + ENDPOINT_PATH
    payload = {
        "conversationType": conversation_type,
        "message": message,
    }

    if edition_id is not None:
        payload["editionId"] = edition_id

    cookie_name = os.environ.get("SESSION_COOKIE_NAME", "vra_session")
    session_cookie = sign_session(test_session_id)

    response = requests.post(
        url,
        json=payload,
        headers=get_vra_auth_headers(),
        cookies={cookie_name: session_cookie},
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


def test_chat_assistant_messages_have_db_ids_matching_db(
    vra_test_user, test_session_id
):
    """Assistant messages in the response should carry a db_id that maps to a
    matching row in agent_messages, with identical message_data content."""
    url = require_env("DRB_API_URL") + ENDPOINT_PATH
    payload = {
        "conversationType": "catalogSearch",
        "message": "Find books about history.",
    }
    cookie_name = os.environ.get("SESSION_COOKIE_NAME", "vra_session")
    session_cookie = sign_session(test_session_id)

    response = requests.post(
        url,
        json=payload,
        headers=get_vra_auth_headers(),
        cookies={cookie_name: session_cookie},
    )
    assert_response_status(url, response, 200)

    # Collect assistant messages
    messages = response.json()["data"]["messages"]
    assistant_messages = [
        m
        for m in messages
        if m.get("role") == "assistant" and m.get("type") == "message"
    ]

    assert len(assistant_messages) > 0, (
        "Expected at least one assistant message in the response"
    )

    missing_db_id = [m for m in assistant_messages if "db_id" not in m]
    assert missing_db_id == [], (
        f"All assistant messages must have a db_id, but {len(missing_db_id)} did not: "
        f"{missing_db_id}"
    )

    # Verify that returned message ids match DB data
    engine = get_engine()
    with engine.connect() as conn:
        for msg in assistant_messages:
            db_id = msg["db_id"]
            row = conn.execute(
                text("SELECT message_data FROM agent_messages WHERE id = :id"),
                {"id": db_id},
            ).fetchone()

            assert row is not None, f"No agent_messages row found for db_id={db_id}"

            expected_content = {k: v for k, v in msg.items() if k != "db_id"}
            assert row.message_data == expected_content, (
                f"message_data in DB does not match response content for db_id={db_id}"
            )
