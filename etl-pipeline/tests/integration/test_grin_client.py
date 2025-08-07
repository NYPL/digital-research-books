import pytest
import random
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession


def test_grin_client_initialization(grin_client):
    assert grin_client is not None
    assert isinstance(grin_client.creds, Credentials)
    assert grin_client.session is not None
    assert isinstance(grin_client.session, AuthorizedSession)


@pytest.mark.parametrize(
    "fragment", ["_available", "_in_process", "_converted", "_failed", "_all_books"]
)
def test_url(grin_client, fragment):
    url = grin_client._url(fragment)

    assert url is not None
    assert isinstance(url, str)
    assert url.startswith("https://books.google.com/libraries/NYPL/")
    assert url.endswith(fragment)


@pytest.mark.parametrize(
    "fragment",
    ["_available", "_converted", "_failed", "_all_books"],
    # removed _in_process because of the 429 too many requests error
)
def test_get(grin_client, fragment):
    url = grin_client._url(fragment)

    response = grin_client.session.request("GET", url)
    assert response is not None
    assert response.status_code == 200


def test_convert(grin_client, generate_test_barcodes, expected_barcodes_statuses):
    response = grin_client.convert(generate_test_barcodes)

    filtered = [item for item in response if item and not item.startswith("Barcode")]
    assert response is not None
    assert len(filtered) == len(generate_test_barcodes)
    assert isinstance(response, list)
    assert all(isinstance(item, str) for item in filtered)
    for item in filtered:
        assert any(
            status_keyword in item for status_keyword in expected_barcodes_statuses
        ), f"Unexpected status: {item}"
