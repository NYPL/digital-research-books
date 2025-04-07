import pytest

from managers.cover_fetchers.abstractFetcher import AbstractFetcher


class TestAbstractFetcher:
    @pytest.fixture
    def testFetcher(self):
        class MockAbstractFetcher(AbstractFetcher):
            def __init__(self, *args):
                super().__init__(*args)

            def hasCover(self):
                return super().hasCover()

            def downloadCoverFile(self):
                return super().downloadCoverFile()

        return MockAbstractFetcher(['id1', 'id2'])

    def test_initializer(self, testFetcher):
        assert testFetcher.identifiers == ['id1', 'id2']
        assert testFetcher.coverID == None

    def test_hasCover(self, testFetcher):
        assert testFetcher.hasCover() == False

    def test_downloadCoverFile(self, testFetcher):
        assert testFetcher.downloadCoverFile() == None
