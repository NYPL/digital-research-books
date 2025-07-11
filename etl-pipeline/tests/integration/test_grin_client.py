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
    BATCHLIMIT = 1
    barcodes = ["33433124887211", "33433124887212", "33433124887213", "33433124887214"]
    response = grin_client.convert(barcodes)

    assert response is not None
    assert len(response) == len(barcodes)
    assert isinstance(response, list)
    assert all(isinstance(item, str) for item in response)
