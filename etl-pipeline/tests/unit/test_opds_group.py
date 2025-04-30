import pytest

from api.opds2.group import Group, OPDS2GroupException


class TestOPDSFacet:
    @pytest.fixture
    def testGroupEls(self, mocker):
        groupMocks = mocker.patch.multiple(
            "api.opds2.group",
            Metadata=mocker.DEFAULT,
            Navigation=mocker.DEFAULT,
            Publication=mocker.DEFAULT,
        )
        return Group(), groupMocks

    @pytest.fixture
    def testIterableClass(self):
        class TestIter:
            def __init__(self, name):
                self.name = name

            def __iter__(self):
                yield "name", self.name

        return TestIter

    def test_initializer(self, testGroupEls, mocker):
        testGroup, groupMocks = testGroupEls

        assert isinstance(testGroup.metadata, mocker.MagicMock)
        assert testGroup.navigation == []
        assert testGroup.publications == []
        groupMocks["Metadata"].assert_called_once()

    def test_addMetadata(self, testGroupEls, mocker):
        testGroup, _ = testGroupEls

        testGroup.addMetadata({"test": "value"})

        assert testGroup.metadata.test == "value"

    def test_addPublication_dict(self, testGroupEls):
        testGroup, groupMocks = testGroupEls

        groupMocks["Publication"].return_value = "testPublication"

        testGroup.addPublication({"title": "Test Pub"})

        assert testGroup.publications[0] == "testPublication"
        groupMocks["Publication"].assert_called_once_with(title="Test Pub")

    def test_addPublication_object(self, testGroupEls, mocker):
        testGroup, _ = testGroupEls

        mockPub = mocker.MagicMock(title="Test Pub")

        testGroup.addPublication(mockPub)

        assert testGroup.publications[0] == mockPub

    def test_addPublications(self, testGroupEls, mocker):
        testGroup, _ = testGroupEls

        mockAddPub = mocker.patch.object(Group, "addPublication")

        testGroup.addPublications(["pub1", "pub2"])

        mockAddPub.assert_has_calls([mocker.call("pub1"), mocker.call("pub2")])

    def test_addNavigation_dict(self, testGroupEls):
        testGroup, groupMocks = testGroupEls

        groupMocks["Navigation"].return_value = "testNavigation"

        testGroup.addNavigation({"title": "Test Nav"})

        assert testGroup.navigation[0] == "testNavigation"
        groupMocks["Navigation"].assert_called_once_with(title="Test Nav")

    def test_addNavigation_object(self, testGroupEls, mocker):
        testGroup, _ = testGroupEls

        mockNav = mocker.MagicMock(title="Test Nav")

        testGroup.addNavigation(mockNav)

        assert testGroup.navigation[0] == mockNav

    def test_addNavigations(self, testGroupEls, mocker):
        testGroup, _ = testGroupEls

        mockAddNav = mocker.patch.object(Group, "addNavigation")

        testGroup.addNavigations(["nav1", "nav2"])

        mockAddNav.assert_has_calls([mocker.call("nav1"), mocker.call("nav2")])

    def test_iter_success(self, testGroupEls, testIterableClass, mocker):
        testGroup, _ = testGroupEls

        testGroup.metadata = testIterableClass("Test Meta")
        testGroup.publications = [testIterableClass("pub1"), testIterableClass("pub2")]

        assert dict(testGroup) == {
            "metadata": {"name": "Test Meta"},
            "publications": [{"name": "pub1"}, {"name": "pub2"}],
        }

    def test_iter_error(self, testGroupEls):
        testGroup, _ = testGroupEls

        testGroup.publications = ["pub1", "pub2"]
        testGroup.navigation = ["nav1"]

        with pytest.raises(OPDS2GroupException):
            dict(testGroup)

    def test_repr(self, testGroupEls, mocker):
        testGroup, _ = testGroupEls

        testGroup.metadata = mocker.MagicMock(title="Test Title")
        testGroup.publications = ["pub1", "pub2", "pub3"]

        assert (
            str(testGroup) == "<Group(title=Test Title, publications=3, navigation=0)>"
        )
