from base64 import b64encode

from utils.common import require_env


def assert_response_status(url: str, response, expected_status_code: int):
    assert response.status_code == expected_status_code, (
        f"API call failed.\n"
        f"Expected status code: {expected_status_code}\n"
        f"Actual status code: {response.status_code}\n"
        f"URL: {url}\n"
        f"Response text: {response.text[:100]}..."
    )


def get_vra_auth_headers():
    api_key = require_env("API_KEY")

    username = require_env("VRA_USERNAME")
    password = require_env("VRA_PASSWORD")
    credentials = f"{username}:{password}".encode("utf-8")
    basic_auth_token = b64encode(credentials).decode("utf-8")

    return {
        "Authorization": f"Basic {basic_auth_token}",
        "X-API-KEY": api_key,
    }
