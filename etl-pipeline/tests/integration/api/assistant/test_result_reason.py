import os

import requests

from ..utils import (
    assert_response_status,
    assert_top_level_response_fields,
    get_vra_auth_headers,
)
from utils.common import require_env
from api.session_jwt import sign_session

CHAT_ENDPOINT_PATH = "/chat"
RESULT_REASON_ENDPOINT_PATH = "/result-reason"

# The result_reason system prompt asks for ~450 characters / 3-4 sentences.
MAX_EXPLANATION_LENGTH = 500


def test_result_reason_happy_path(vra_test_user, test_session_id):
    """
    End-to-end happy path: a real /chat catalogSearch call seeds a session
    with a search tool call_id + edition_id, then /result-reason explains why
    that book appeared, using a real (non-mocked) LLM call.
    """
    base_url = require_env("DRB_API_URL")
    session = requests.Session()
    session.headers.update(get_vra_auth_headers())

    cookie_name = os.environ.get("SESSION_COOKIE_NAME", "vra_session")
    session_cookie = sign_session(test_session_id)
    session.cookies.set(cookie_name, session_cookie)

    # Make /chat request
    chat_url = base_url + CHAT_ENDPOINT_PATH
    chat_response = session.post(
        chat_url,
        json={
            "conversationType": "catalogSearch",
            "message": "Find something on fire.",
        },
        timeout=90,
    )
    assert_response_status(chat_url, chat_response, 200)
    chat_data = chat_response.json()["data"]

    # Extract /result-reason request param data from first result returned by /chat
    search_result = chat_data["search_result"]
    assert search_result is not None, (
        f"No search_result found in /chat response: {chat_data}"
    )

    call_id = search_result["tool_call_id"]
    assert call_id is not None, (
        f"No search tool call found in /chat response: {chat_data}"
    )

    editions = search_result["results"]["editions"]
    assert editions, (
        f"No editions found in /chat response search_result: {search_result}"
    )
    edition_id = editions[0]["id"]  # first result returned by /chat
    assert edition_id is not None, (
        f"No edition id found on first edition in /chat response: {editions[0]}"
    )

    # Make /result-reason request
    result_reason_url = base_url + RESULT_REASON_ENDPOINT_PATH
    result_reason_response = session.post(
        result_reason_url,
        json={"call_id": call_id, "edition_id": edition_id},
        timeout=90,
    )

    # Core test assertions
    assert_response_status(result_reason_url, result_reason_response, 200)

    try:
        response_json = result_reason_response.json()
    except ValueError:
        response_json = None
        assert response_json is not None, "Response is not valid JSON"

    assert_top_level_response_fields(response_json)

    data = response_json["data"]
    assert isinstance(data["explanation"], str) and data["explanation"], (
        "explanation must be a non-empty string"
    )
    assert len(data["explanation"]) < MAX_EXPLANATION_LENGTH, (
        f"Explanation exceeds {MAX_EXPLANATION_LENGTH} characters "
        f"({len(data['explanation'])}): {data['explanation']}"
    )
    assert data["is_ai_generated"] is True
    assert data["session_id"] == chat_data["session_id"]
