import pytest
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession



@pytest.fixture()
def barcodes():
    return [
        "33433116191085",
        "33433115134839",
        "33433109238562",  
        "33433109238562",
        " ",
        "fedk4094043803+_"
    ]


def test_grin_client_initialization(grin_client):
    assert grin_client is not None
    assert isinstance(grin_client.creds, Credentials)
    assert grin_client.client.session is not None
    assert isinstance(grin_client.session, AuthorizedSession)



@pytest.mark.parametrize(
    "fragment", ["_available", "_in_process", "_converted", "_failed", "_all_books"]
)
def test_url(self, grin_client, fragment):
        url = grin_client._url(fragment)

        assert url is not None
        assert isinstance(url, str)
        assert url.startswith("https://books.google.com/libraries/NYPL/")
        assert url.endswith(fragment)



@pytest.mark.parametrize(
    "fragment", ["_available", "_in_process", "_converted", "_failed", "_all_books"]
)
        
def test_get(self, grin_client, fragment):
        url = grin_client._url(fragment)

        response = grin_client.session.request("GET", url)
        assert response is not None
        assert response.status_code == 200


def test_convert(grin_client, barcodes):
    response = grin_client.convert(barcodes)

    filtered = [item for item in response if item and not item.startswith("Barcode")]
    assert response is not None
    assert len(filtered) == len(barcodes)
    assert isinstance(response, list)
    assert all(isinstance(item, str) for item in filtered)
    for item in filtered:
        if "Already being converted" in item:
             raise ValueError(f"Barcode already converted: {item}")
        elif "Not allowed to be downloaded"  in item:
             raise ValueError(f"Barcode conversion failed: {item}")

        elif "Other error" in item:
            raise ValueError(f"Barcode conversion error: {item}")
        