import pytest
import random
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession


@pytest.fixture()
def barcodes(grin_client):
    available_barcodes_scrape_fragment = "_all_books?&book_state=NEW&format=text"
    byte_response = grin_client.get(available_barcodes_scrape_fragment)
    lines = byte_response.decode("utf8").strip().split("\n")
    filtered_barcodes = [
        b.strip() for b in lines if b.strip().isdigit() and len(b.strip()) == 14
    ]

    return (
        random.sample(filtered_barcodes, 5)
        if len(filtered_barcodes) >= 5
        else filtered_barcodes
    )


@pytest.fixture()
def expected_barcodes_statuses():
    return [
        "Success",
        "Already being converted",
        "Not allowed to be downloaded",
        "Other error",
    ]


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
    "fragment", ["_available", "_in_process", "_converted", "_failed", "_all_books"]
)
def test_get(grin_client, fragment):
    url = grin_client._url(fragment)

    response = grin_client.session.request("GET", url)
    assert response is not None
    assert response.status_code == 200


def test_convert(grin_client, barcodes, expected_barcodes_statuses):
    response = grin_client.convert(barcodes)

    filtered = [item for item in response if item and not item.startswith("Barcode")]
    assert response is not None
    assert len(filtered) == len(barcodes)
    assert isinstance(response, list)
    assert all(isinstance(item, str) for item in filtered)
    for item in filtered:
        assert any(
            status_keyword in item for status_keyword in expected_barcodes_statuses
        ), f"Unexpected status: {item}"
