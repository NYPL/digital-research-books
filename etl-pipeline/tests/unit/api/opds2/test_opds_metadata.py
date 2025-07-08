import pytest

from api.opds2.metadata import Metadata, OPDS2MetadataException


class TestMetadata:
    def test_initializer(self):
        testMeta = Metadata(title="test")

        assert testMeta.attrs == set(["title"])
        assert testMeta.title == "test"

    def test_addField(self):
        testMeta = Metadata()

        testMeta.addField("title", "Test Title")

        assert testMeta.attrs == set(["title"])
        assert testMeta.title == "Test Title"

    def test_addFields_dict(self, mocker):
        testMeta = Metadata()

        mockAdd = mocker.patch.object(Metadata, "addField")

        testMeta.addFields({"title": "test", "identifier": "testID"})

        mockAdd.assert_has_calls(
            [mocker.call("title", "test"), mocker.call("identifier", "testID")]
        )

    def test_addFields_list(self, mocker):
        testMeta = Metadata()

        mockAdd = mocker.patch.object(Metadata, "addField")

        testMeta.addFields([("title", "test"), ("identifier", "testID")])

        mockAdd.assert_has_calls(
            [mocker.call("title", "test"), mocker.call("identifier", "testID")]
        )

    def test_iter_success(self):
        testMeta = Metadata(title="Test", type="testing")

        assert dict(testMeta) == {"title": "Test", "type": "testing"}

    def test_iter_error_missing_field(self):
        testMeta = Metadata(other="testing")

        with pytest.raises(OPDS2MetadataException):
            dict(testMeta)

    def test_repr(self):
        testMeta = Metadata(title="Test Title")

        assert str(testMeta) == "<Metadata(title=Test Title)>"
