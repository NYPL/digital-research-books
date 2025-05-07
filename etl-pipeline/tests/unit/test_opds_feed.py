import pytest

from api.opds2.feed import Feed, OPDS2FeedException


class TestOPDSFacet:
    @pytest.fixture
    def testFeed(self, mocker):
        return Feed()

    @pytest.fixture
    def testIterableClass(self):
        class TestIter:
            def __init__(self, rel):
                self.rel = rel

            def __iter__(self):
                yield "rel", self.rel

        return TestIter

    def test_initializer(self, testFeed):
        assert testFeed.metadata is None
        assert testFeed.navigation is None
        assert testFeed.links == []
        assert testFeed.publications == []
        assert testFeed.groups == []
        assert testFeed.images == []
        assert testFeed.facets == []

    def test_addMetadata(self, testFeed, mocker):
        mockComponentize = mocker.patch.object(Feed, "componentizeObject")
        mockComponentize.return_value = "testMetadata"

        testFeed.addMetadata({"test": "value"})

        assert testFeed.metadata == "testMetadata"

    def test_addNavigation(self, testFeed, mocker):
        mockComponent = mocker.patch.object(Feed, "addComponent")

        testFeed.addNavigation("testNav")

        mockComponent.assert_called_once_with("navigation", "testNav")

    def test_addNavigations(self, testFeed, mocker):
        mockAddNav = mocker.patch.object(Feed, "addNavigation")

        testFeed.addNavigations(["nav1", "nav2"])

        mockAddNav.assert_has_calls([mocker.call("nav1"), mocker.call("nav2")])

    def test_addLink(self, testFeed, mocker):
        mockComponent = mocker.patch.object(Feed, "addComponent")

        testFeed.addLink("testLink")

        mockComponent.assert_called_once_with("links", "testLink")

    def test_addLinks(self, testFeed, mocker):
        mockAddLink = mocker.patch.object(Feed, "addLink")

        testFeed.addLinks(["link1", "link2"])

        mockAddLink.assert_has_calls([mocker.call("link1"), mocker.call("link2")])

    def test_addPublication(self, testFeed, mocker):
        mockComponent = mocker.patch.object(Feed, "addComponent")

        testFeed.addPublication("testPub")

        mockComponent.assert_called_once_with("publications", "testPub")

    def test_addPublications(self, testFeed, mocker):
        mockAddPub = mocker.patch.object(Feed, "addPublication")

        testFeed.addPublications(["pub1", "pub2"])

        mockAddPub.assert_has_calls([mocker.call("pub1"), mocker.call("pub2")])

    def test_addGroup(self, testFeed, mocker):
        mockComponent = mocker.patch.object(Feed, "addComponent")

        testFeed.addGroup("testGroup")

        mockComponent.assert_called_once_with("groups", "testGroup")

    def test_addGroups(self, testFeed, mocker):
        mockAddGroup = mocker.patch.object(Feed, "addGroup")

        testFeed.addGroups(["group1", "group2"])

        mockAddGroup.assert_has_calls([mocker.call("group1"), mocker.call("group2")])

    def test_addImage(self, testFeed, mocker):
        mockComponent = mocker.patch.object(Feed, "addComponent")

        testFeed.addImage("testImg")

        mockComponent.assert_called_once_with("images", "testImg")

    def test_addImages(self, testFeed, mocker):
        mockAddImage = mocker.patch.object(Feed, "addImage")

        testFeed.addImages(["img1", "img2"])

        mockAddImage.assert_has_calls([mocker.call("img1"), mocker.call("img2")])

    def test_addFacet(self, testFeed, mocker):
        mockComponent = mocker.patch.object(Feed, "addComponent")

        testFeed.addFacet("testFacet")

        mockComponent.assert_called_once_with("facets", "testFacet")

    def test_addFacets(self, testFeed, mocker):
        mockAddFacet = mocker.patch.object(Feed, "addFacet")

        testFeed.addFacets(["facet1", "facet2"])

        mockAddFacet.assert_has_calls([mocker.call("facet1"), mocker.call("facet2")])

    def test_addComponent_new(self, testFeed, mocker):
        testFeed.test = None

        mockComponentize = mocker.patch.object(Feed, "componentizeObject")

        mockComponentize.return_value = "testComponent"

        testFeed.addComponent("test", "testComponent")

        assert testFeed.test == ["testComponent"]

    def test_addComponent_existing(self, testFeed, mocker):
        testFeed.test = ["existingComponent"]

        mockComponentize = mocker.patch.object(Feed, "componentizeObject")
        mockComponentize.return_value = "testComponent"

        testFeed.addComponent("test", "testComponent")

        assert testFeed.test == ["existingComponent", "testComponent"]

    def test_dir(self, testFeed):
        assert dir(testFeed) == [
            "facets",
            "groups",
            "images",
            "links",
            "metadata",
            "navigation",
            "publications",
        ]

    def test_iter_success(self, testFeed, testIterableClass, mocker):
        testFeed.links = [testIterableClass("self")]
        testFeed.metadata = {"title": "test", "other": "value"}

        assert dict(testFeed) == {
            "links": [{"rel": "self"}],
            "metadata": {"title": "test", "other": "value"},
        }

    def test_iter_error(self, testFeed, testIterableClass):
        testFeed.links = [testIterableClass("other")]

        with pytest.raises(OPDS2FeedException):
            dict(testFeed)
