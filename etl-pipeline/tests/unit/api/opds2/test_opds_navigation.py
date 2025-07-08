import pytest

from api.opds2.navigation import Navigation, OPDS2NavigationException


class TestNavigation:
    def test_initializer(self):
        testNav = Navigation()

        assert testNav.href == None
        assert testNav.title == None
        assert testNav.rel == None
        assert testNav.type == None

    def test_addField(self):
        testNav = Navigation()

        testNav.addField("title", "Test Title")

        assert testNav.title == "Test Title"

    def test_addFields_dict(self, mocker):
        testNav = Navigation()

        mockAdd = mocker.patch.object(Navigation, "addField")

        testNav.addFields({"title": "test", "href": "testURL"})

        mockAdd.assert_has_calls(
            [mocker.call("title", "test"), mocker.call("href", "testURL")]
        )

    def test_addFields_list(self, mocker):
        testNav = Navigation()

        mockAdd = mocker.patch.object(Navigation, "addField")

        testNav.addFields([("title", "test"), ("href", "testURL")])

        mockAdd.assert_has_calls(
            [mocker.call("title", "test"), mocker.call("href", "testURL")]
        )

    def test_iter_success(self):
        testNav = Navigation(title="Test", type="testing")

        assert dict(testNav) == {
            "title": "Test",
            "type": "testing",
            "rel": None,
            "href": None,
        }

    def test_iter_error_missing_field(self):
        testNav = Navigation(other="testing")

        with pytest.raises(OPDS2NavigationException):
            dict(testNav)

    def test_repr(self):
        testNav = Navigation(title="Test Title")

        assert str(testNav) == "<Navigation(title=Test Title, type=None, href=None)>"

    def test_dir(self):
        testNav = Navigation()

        assert dir(testNav) == ["href", "rel", "title", "type"]
