import pytest

from api.opds2.link import Link, OPDS2LinkException


class TestLink:
    def test_initializer(self):
        testLink = Link(href="testURL", rel="test")

        assert testLink.attrs == set(["href", "rel"])
        assert testLink.href == "testURL"
        assert testLink.rel == "test"

    def test_addField(self):
        testLink = Link()

        testLink.addField("href", "testURL")

        assert testLink.attrs == set(["href"])
        assert testLink.href == "testURL"

    def test_addFields_dict(self, mocker):
        testLink = Link()

        mockAdd = mocker.patch.object(Link, "addField")

        testLink.addFields({"href": "testURL", "rel": "test"})

        mockAdd.assert_has_calls(
            [mocker.call("href", "testURL"), mocker.call("rel", "test")]
        )

    def test_addFields_list(self, mocker):
        testLink = Link()

        mockAdd = mocker.patch.object(Link, "addField")

        testLink.addFields([("href", "testURL"), ("rel", "test")])

        mockAdd.assert_has_calls(
            [mocker.call("href", "testURL"), mocker.call("rel", "test")]
        )

    def test_iter_success(self):
        testLink = Link(href="testURL", title="Test", type="testing")

        assert dict(testLink) == {"href": "testURL", "title": "Test", "type": "testing"}

    def test_iter_error_missing_field(self):
        testLink = Link(title="testing")

        with pytest.raises(OPDS2LinkException):
            dict(testLink)

    def test_iter_error_unpermitted_field(self):
        testLink = Link(other="testing")

        with pytest.raises(OPDS2LinkException):
            dict(testLink)

    def test_repr(self):
        testLink = Link(href="testURL", rel="test")

        assert str(testLink) == "<Link(href=testURL, rel=test)>"
