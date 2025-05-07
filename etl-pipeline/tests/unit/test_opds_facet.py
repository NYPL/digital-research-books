import pytest

from api.opds2 import Facet


class TestOPDSFacet:
    @pytest.fixture
    def testFacet(self, mocker):
        mockMeta = mocker.patch("api.opds2.facet.Metadata")
        mockMeta.return_value = mocker.MagicMock(test="meta")
        mockLink = mocker.patch("api.opds2.facet.Link")
        mockLink.side_effect = ["link1", "link2"]

        return Facet(
            metadata={"test": "meta"}, links=[{"url": "test1"}, {"url": "test2"}]
        )

    def test_initializer(self, testFacet):
        assert testFacet.metadata.test == "meta"
        assert testFacet.links == ["link1", "link2"]

    def test_addMetadata(self, testFacet):
        testFacet.addMetadata({"other": "value"})

        assert testFacet.metadata.other == "value"

    def test_addLinks(self, testFacet, mocker):
        mockAdd = mocker.patch.object(Facet, "addLink")

        testFacet.addLinks([1, 2, 3])

        mockAdd.assert_has_calls([mocker.call(i) for i in range(1, 4)])

    def test_addLink_dict(self, testFacet, mocker):
        mockLink = mocker.patch("api.opds2.facet.Link")
        mockLink.return_value = "testLink"

        testFacet.addLink({"test": "value"})

        mockLink.assert_called_once_with(test="value")
        assert testFacet.links[2] == "testLink"

    def test_addLink_object(self, testFacet, mocker):
        mockLink = mocker.MagicMock()

        testFacet.addLink(mockLink)

        assert testFacet.links[2] == mockLink

    def test_dir(self, testFacet):
        assert dir(testFacet) == ["links", "metadata"]

    def test_iter(self):
        testFacet = Facet()
        testFacet.metadata = {"test": "meta"}
        testFacet.links = [[("url", "link1")], [("url", "link2")]]
        assert dict(testFacet) == {
            "metadata": {"test": "meta"},
            "links": [{"url": "link1"}, {"url": "link2"}],
        }

    def test_repr(self, testFacet):
        testFacet.metadata.title = "test"

        assert str(testFacet) == "<Facet(title=test, links=2)>"
