import os

import requests
from lxml import etree as ET

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


def find_search_tool_call_and_barcode(messages):
    """
    Given the `messages` list returned by /chat, find the last search tool
    call's (call_id, output) pair and the first barcode present in its XML
    output. Returns (call_id, barcode); either may be None if not found.
    """
    call_id = None
    tool_output = None
    for msg in messages:
        if msg.get("type") == "function_call" and msg.get("name") in (
            "search_catalog",
            "search_book",
        ):
            call_id = msg.get("call_id")
        elif (
            msg.get("type") == "function_call_output" and msg.get("call_id") == call_id
        ):
            tool_output = msg.get("output", "")

    if call_id is None or not tool_output:
        return call_id, None

    try:
        root = ET.fromstring(tool_output)
        barcode_el = root.find(
            ".//barcode"
        )  # Q: there are multiple barcodes in the output. which is this?
    except ET.XMLSyntaxError:
        return call_id, None

    return call_id, (barcode_el.text if barcode_el is not None else None)


# NOTE: future tool calls and outputs may not be in /chat response. instead
# look up convo history from DB with session ID.
def test_result_reason_happy_path(vra_test_user, test_session_id):
    """
    End-to-end happy path: a real /chat catalogSearch call seeds a session
    with a search tool call_id + barcode, then /result-reason explains why
    that book appeared, using a real (non-mocked) LLM call.
    """
    base_url = require_env("DRB_API_URL")
    session = requests.Session()
    session.headers.update(get_vra_auth_headers())

    cookie_name = os.environ.get("SESSION_COOKIE_NAME", "vra_session")
    session_cookie = sign_session(test_session_id)
    session.cookies.set(cookie_name, session_cookie)

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

    call_id, barcode = find_search_tool_call_and_barcode(chat_data["messages"])
    assert call_id is not None, (
        f"No search tool call found in /chat response messages: {chat_data['messages']}"
    )
    assert barcode is not None, (
        f"No barcode found in search tool output for call_id '{call_id}'"
    )

    result_reason_url = base_url + RESULT_REASON_ENDPOINT_PATH
    result_reason_response = session.post(
        result_reason_url,
        json={"call_id": call_id, "barcode": barcode},
        timeout=90,
    )

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
