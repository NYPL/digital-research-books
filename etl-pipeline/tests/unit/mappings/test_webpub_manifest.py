import json
import pytest

from tests.helper import TestHelpers
from managers import WebpubManifest


class TestPDFManifest:
    @classmethod
    def setup_class(cls):
        TestHelpers.setEnvVars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clearEnvVars()

    @pytest.fixture
    def testManifest(self):
        return WebpubManifest("testSource", "testMediaType")

    def test_initializer(self, testManifest):
        assert testManifest.metadata == {"@type": "https://schema.org/Book"}
        assert testManifest.links == [
            {"href": "testSource", "type": "testMediaType", "rel": "alternate"}
        ]
        assert testManifest.readingOrder == []
        assert testManifest.resources == set()
        assert testManifest.tableOfContents == []
        assert testManifest.openSection is None

    def test_addMetadata(self, testManifest, mocker):
        mockRecord = mocker.MagicMock()
        mockRecord.title = "Test Record"
        mockRecord.authors = ["Author 1|||true", "Author 2|||false"]
        mockRecord.identifiers = ["id1|test", "id2|isbn"]

        testManifest.addMetadata(mockRecord, conformsTo="test_profile_uri")

        assert testManifest.metadata == {
            "@type": "https://schema.org/Book",
            "title": "Test Record",
            "author": "Author 1",
            "identifier": "urn:isbn:id2",
            "conformsTo": "test_profile_uri",
        }

    def test_addSection_no_existing(self, testManifest):
        testManifest.addSection("New Section", "sectionURL")

        assert testManifest.openSection == {
            "href": "sectionURL",
            "title": "New Section",
            "children": [],
        }

    def test_addSection_existing(self, testManifest):
        section1 = {"href": "sectionURL_1", "title": "Section 1", "children": []}
        testManifest.openSection = section1

        testManifest.addSection("Section 2", "sectionURL_2")

        assert testManifest.tableOfContents[0] == section1
        assert testManifest.openSection == {
            "href": "sectionURL_2",
            "title": "Section 2",
            "children": [],
        }

    def test_closeSection(self, testManifest):
        testSection = {"href": "sectionURL", "title": "Test Section", "children": []}
        testManifest.openSection = testSection

        testManifest.closeSection()

        assert testManifest.openSection is None
        assert testManifest.tableOfContents[0] == testSection

    def test_addChapter_direct_no_subsections(self, testManifest, mocker):
        mockAddOrder = mocker.patch.object(WebpubManifest, "addReadingOrder")
        mockAddRes = mocker.patch.object(WebpubManifest, "addResource")

        testManifest.addChapter("testLink", "testTitle")

        assert testManifest.tableOfContents[0] == {
            "href": "testLink",
            "title": "testTitle",
        }
        mockAddOrder.assert_called_once_with({"href": "testLink", "title": "testTitle"})
        mockAddRes.assert_called_once_with("testLink")

    def test_addChapter_parent_section_no_subsections(self, testManifest, mocker):
        mockAddOrder = mocker.patch.object(WebpubManifest, "addReadingOrder")
        mockAddRes = mocker.patch.object(WebpubManifest, "addResource")

        testManifest.openSection = {
            "href": "testSection",
            "title": "Section 1",
            "children": [],
        }

        testManifest.addChapter("testLink", "testTitle")

        assert testManifest.openSection["children"][0] == {
            "href": "testLink",
            "title": "testTitle",
        }
        mockAddOrder.assert_called_once_with({"href": "testLink", "title": "testTitle"})
        mockAddRes.assert_called_once_with("testLink")

    def test_addChapter_direct_w_subsections(self, testManifest, mocker):
        mockAddOrder = mocker.patch.object(WebpubManifest, "addReadingOrder")
        mockAddRes = mocker.patch.object(WebpubManifest, "addResource")

        testManifest.addChapter("testLink", "testTitle", subsections=[1, 2, 3])

        assert testManifest.tableOfContents[0] == {
            "href": "testLink",
            "title": "testTitle",
            "children": [1, 2, 3],
        }
        mockAddOrder.assert_called_once_with({"href": "testLink", "title": "testTitle"})
        mockAddRes.assert_called_once_with("testLink")

    def test_addReadingOrder(self, testManifest):
        testManifest.addReadingOrder({"href": "testLink"})

        assert testManifest.readingOrder[0] == {
            "href": "testLink",
            "type": "application/pdf",
        }

    def test_addResource_noPath(self, testManifest):
        testManifest.addResource("testLink")

        assert testManifest.resources == set(["testLink"])

    def test_addResource_path_non_duplicate(self, testManifest):
        testManifest.resources.add("otherLink")
        testManifest.addResource("testLink#page=1")

        assert testManifest.resources == set(["otherLink", "testLink"])

    def test_addResource_path_duplicate(self, testManifest):
        testManifest.resources.add("testLink")
        testManifest.addResource("testLink#page=1")

        assert testManifest.resources == set(["testLink"])

    def test_toDict(self, testManifest, mocker):
        testManifest.readingOrder = ["order1", "order2", "order3"]
        testManifest.resources = ["res1", "res2"]

        assert testManifest.toDict() == {
            "context": "https://test_aws_bucket-s3.amazonaws.com/manifests/context.jsonld",
            "metadata": {"@type": "https://schema.org/Book"},
            "links": [
                {"href": "testSource", "type": "testMediaType", "rel": "alternate"}
            ],
            "readingOrder": ["order1", "order2", "order3"],
            "resources": [
                {"href": "res1", "type": "application/pdf"},
                {"href": "res2", "type": "application/pdf"},
            ],
            "toc": [],
        }

    def test_toJson(self, testManifest):
        jsonManifest = testManifest.toJson()
        assert isinstance(jsonManifest, str)
        assert (
            json.loads(jsonManifest)["context"]
            == "https://test_aws_bucket-s3.amazonaws.com/manifests/context.jsonld"
        )
