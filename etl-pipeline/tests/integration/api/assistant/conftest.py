from base64 import b64encode

import pytest

from utils.common import require_env


@pytest.fixture(scope="session")
def auth_headers(setup_env):
    api_key = require_env("API_KEY")

    username = require_env("VRA_USERNAME")
    password = require_env("VRA_PASSWORD")
    credentials = f"{username}:{password}".encode("utf-8")
    basic_auth_token = b64encode(credentials).decode("utf-8")

    return {
        "Authorization": f"Basic {basic_auth_token}",
        "X-API-KEY": api_key,
    }
