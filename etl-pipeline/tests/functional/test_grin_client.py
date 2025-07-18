import pytest


@pytest.mark.parametrize(
    "fragment", ["_available", "_in_process", "_converted", "_failed", "_all_books"]
)
class TestGRINClient:
    def test_url(self, grin_client, fragment):
        url = grin_client._url(fragment)

        assert url is not None
        assert isinstance(url, str)
        assert url.startswith("https://books.google.com/libraries/NYPL/")

    def test_get(self, grin_client, fragment):
        url = grin_client._url(fragment)

        response = grin_client.session.request("GET", url)
        assert response is not None
        assert response.status_code == 200


def test_convert(grin_client):
    barcodes = ["33433116084322", "33433116012059", "33433116012034"]
    response = grin_client.convert(barcodes)

    filtered = [item for item in response if item and not item.startswith("Barcode")]
    assert response is not None
    assert len(filtered) == len(barcodes)
    assert isinstance(response, list)
    assert all(isinstance(item, str) for item in filtered)
    for item in filtered:
        if "Success" not in item:
            print(f"Warning: Barcode conversion not successful: {item}")
