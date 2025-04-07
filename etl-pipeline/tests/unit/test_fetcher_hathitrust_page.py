import pytest

from managers.cover_fetchers.hathiFetcher import HathiPage


class TestHathiPage:
    @pytest.fixture
    def testPage(self, mocker):
        class MockHathiPage(HathiPage):
            def __init__(self, data):
                self.pageData = data
        
        return MockHathiPage({})
    
    def test_initializer(self, mocker):
        mocker.patch.multiple(HathiPage,
            getPageNumber=mocker.DEFAULT,
            getPageFlags=mocker.DEFAULT,
            getPageScore=mocker.DEFAULT
        )

        testPage = HathiPage({})

        assert testPage.pageData == {}
        assert isinstance(testPage.pageNumber, mocker.MagicMock)
        assert isinstance(testPage.pageFlags, mocker.MagicMock)
        assert isinstance(testPage.pageScore, mocker.MagicMock)

    def test_getPageNumber_present(self, testPage):
        testPage.pageData = {'ORDER': 10}

        assert testPage.getPageNumber() == 10

    def test_getPageNumber_missing(self, testPage):
        assert testPage.getPageNumber() == 0

    def test_getPageFlags_present(self, testPage):
        testPage.pageData = {'LABEL': 'TESTING, OTHER'}

        assert testPage.getPageFlags() == set(['TESTING', 'OTHER'])

    def test_getpageFlags_missing(self, testPage):
        assert testPage.getPageFlags() == set([''])

    def test_getPageScore_present(self, testPage):
        testPage.pageFlags = set(['FRONT_COVER', 'IMAGE_ON_PAGE'])

        assert testPage.getPageScore() == 2

    def test_getPageScore_missing(self, testPage):
        testPage.pageFlags = set([])

        assert testPage.getPageScore() == 0
