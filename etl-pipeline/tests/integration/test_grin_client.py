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
        response_content = grin_client.session.request("GET", url)
        assert response_content is not None
        assert response_content.status_code == 200
