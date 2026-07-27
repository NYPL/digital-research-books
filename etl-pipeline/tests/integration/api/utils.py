from base64 import b64encode

from utils.common import require_env

TOP_LEVEL_RESPONSE_FIELDS = {
    "status": int,
    "timestamp": str,
    "responseType": str,
    "data": dict,
}


def assert_top_level_response_fields(response_json: dict):
    for field, expected_type in TOP_LEVEL_RESPONSE_FIELDS.items():
        assert field in response_json, f"Missing expected top-level field: {field}"
        assert isinstance(response_json[field], expected_type), (
            f"Expected {field} to be of type {expected_type.__name__}"
        )


def assert_response_status(url: str, response, expected_status_code: int):
    assert response.status_code == expected_status_code, (
        f"API call failed.\n"
        f"Expected status code: {expected_status_code}\n"
        f"Actual status code: {response.status_code}\n"
        f"URL: {url}\n"
        f"Response text: {response.text[:100]}..."
    )


def get_vra_auth_headers():
    api_key = require_env("VRA_API_KEY")

    # username = require_env("VRA_TEST_USERNAME")
    username = "vra_integration_test_user"
    password = require_env("VRA_TEST_PASSWORD")
    credentials = f"{username}:{password}".encode("utf-8")
    basic_auth_token = b64encode(credentials).decode("utf-8")

    return {
        "Authorization": f"Basic {basic_auth_token}",
        "X-API-KEY": api_key,
    }
