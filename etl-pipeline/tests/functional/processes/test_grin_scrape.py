from processes.grin.grin_client import GRINClient


class TestGRINScrape:
    def test_url_and_request(self):
        GRIN_SCRAPE_URL = "_all_books?book_state=NEW&book_state=PREVIOUSLY_DOWNLOADED&format=text"  # why previously downloaded?

        grin_client = GRINClient()
        expected_url = grin_client._url(GRIN_SCRAPE_URL)
        response = grin_client.session.request("GET", expected_url, timeout=600)
        response.raise_for_status()  # raises an error for bad responses, otherwise continues

        assert GRIN_SCRAPE_URL in expected_url
        assert response.content, (
            "No content returned from GRIN endpoint"
        )  # assert the response is not empty
