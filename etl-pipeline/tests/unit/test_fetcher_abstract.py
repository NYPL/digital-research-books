import pytest

from managers.cover_fetchers.fetcher_abc import FetcherABC


class TestFetcherABC:
    @pytest.fixture
    def test_fetcher(self):
        class MockFetcherABC(FetcherABC):
            def __init__(self, *args):
                super().__init__(*args)

            def has_cover(self):
                return super().has_cover()

            def download_cover_file(self):
                return super().download_cover_file()

        return MockFetcherABC(['id1', 'id2'])

    def test_initializer(self, test_fetcher):
        assert test_fetcher.identifiers == ['id1', 'id2']
        assert test_fetcher.cover_id == None

    def test_has_cover(self, test_fetcher):
        assert test_fetcher.has_cover() == False

    def test_download_cover_file(self, test_fetcher):
        assert test_fetcher.download_cover_file() == None
