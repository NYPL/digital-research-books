import pytest
from tests.helper import TestHelpers

from api.opds2.publication import Publication, OPDS2PublicationException


class TestOPDSPublication:
    @classmethod
    def setup_class(cls):
        TestHelpers.setEnvVars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clearEnvVars()

    @pytest.fixture
    def testPubEls(self, mocker):
        pubMocks = mocker.patch.multiple(
            "api.opds2.publication",
            Metadata=mocker.DEFAULT,
            Link=mocker.DEFAULT,
            Image=mocker.DEFAULT,
        )
        return Publication(), pubMocks

    @pytest.fixture
    def testIterableClass(self):
        class TestIter:
            def __init__(self, name):
                self.name = name

            def __iter__(self):
                yield "name", self.name

        return TestIter

    def test_initializer(self, testPubEls, mocker):
        testPub, pubMocks = testPubEls

        assert isinstance(testPub.metadata, mocker.MagicMock)
        assert testPub.links == []
        assert testPub.images == []
        assert testPub.editions == []
        pubMocks["Metadata"].assert_called_once()

    def test_addMetadata(self, testPubEls):
        testPub, _ = testPubEls

        testPub.addMetadata({"test": "value"})

        assert testPub.metadata.test == "value"

    def test_addLink_dict(self, testPubEls):
        testPub, pubMocks = testPubEls

        pubMocks["Link"].return_value = "testLink"

        testPub.addLink({"href": "testURL"})

        assert testPub.links[0] == "testLink"
        pubMocks["Link"].assert_called_once_with(href="testURL")

    def test_addLink_object(self, testPubEls, mocker):
        testPub, pubMocks = testPubEls

        mockLink = mocker.MagicMock(href="testURL")

        testPub.addLink(mockLink)

        assert testPub.links[0] == mockLink
        pubMocks["Link"].assert_not_called()

    def test_addLinks(self, testPubEls, mocker):
        testPub, _ = testPubEls

        mockAdd = mocker.patch.object(Publication, "addLink")

        testPub.addLinks(["link1", "link2"])

        mockAdd.assert_has_calls([mocker.call("link1"), mocker.call("link2")])

    def test_addImage_dict(self, testPubEls):
        testPub, pubMocks = testPubEls

        pubMocks["Image"].return_value = "testImage"

        testPub.addImage({"href": "testURL"})

        assert testPub.images[0] == "testImage"
        pubMocks["Image"].assert_called_once_with(href="testURL")

    def test_addImage_object(self, testPubEls, mocker):
        testPub, pubMocks = testPubEls

        mockImage = mocker.MagicMock(href="testURL")

        testPub.addImage(mockImage)

        assert testPub.images[0] == mockImage
        pubMocks["Image"].assert_not_called()

    def test_addImages(self, testPubEls, mocker):
        testPub, _ = testPubEls

        mockAdd = mocker.patch.object(Publication, "addImage")

        testPub.addImages(["img1", "img2"])

        mockAdd.assert_has_calls([mocker.call("img1"), mocker.call("img2")])

    def test_addEdition_object(self, testPubEls, mocker):
        testPub, _ = testPubEls

        mockEdition = mocker.MagicMock(title="Test Edition")

        testPub.addEdition(mockEdition)

        assert testPub.editions[0] == mockEdition

    def test_addEditions(self, testPubEls, mocker):
        testPub, _ = testPubEls

        mockAdd = mocker.patch.object(Publication, "addEdition")

        testPub.addEditions(["ed1", "ed2"])

        mockAdd.assert_has_calls([mocker.call("ed1"), mocker.call("ed2")])

    def test_parseWorkToPublication(self, testPubEls, mocker):
        testPub, _ = testPubEls

        testWork = mocker.MagicMock(
            title="Test Title",
            sub_title="Test Sub",
            alt_titles=["alt1", "alt2"],
            identifiers=["id1", "id2", "id3"],
            authors=[{"name": "Test Author"}, {"name": "Other Author"}],
            contributors=[{"name": "Test Contrib"}],
            languages=[None, {"iso_3": "tst"}, {"iso_3": "oth"}],
            date_created="testCreated",
            date_modified="testModified",
            subjects=[{"heading": "Test Subject"}],
            editions=["ed1", "ed2", "ed3"],
            uuid="testUUID",
        )

        pubMocks = mocker.patch.multiple(
            Publication,
            addLink=mocker.DEFAULT,
            setContributors=mocker.DEFAULT,
            setBestIdentifier=mocker.DEFAULT,
            setPreferredLink=mocker.DEFAULT,
            findAndAddCover=mocker.DEFAULT,
            parseEditions=mocker.DEFAULT,
        )

        testPub.parseWorkToPublication(testWork, searchResult=False)

        testPub.metadata.addField.assert_has_calls(
            [
                mocker.call("title", "Test Title"),
                mocker.call("sortAs", "test title"),
                mocker.call("subtitle", "Test Sub"),
                mocker.call("alternate", ["alt1", "alt2"]),
                mocker.call("author", "Test Author, Other Author"),
                mocker.call("language", "tst,oth"),
                mocker.call("created", "testCreated"),
                mocker.call("modified", "testModified"),
                mocker.call("subject", "Test Subject"),
            ]
        )

        pubMocks["addLink"].assert_called_once_with(
            {
                "href": "/opds/publication/testUUID",
                "rel": "self",
                "type": "application/opds-publication+json",
            }
        )
        pubMocks["setContributors"].assert_called_once_with([{"name": "Test Contrib"}])
        pubMocks["setBestIdentifier"].assert_called_once_with(["id1", "id2", "id3"])
        pubMocks["setPreferredLink"].assert_called_once_with(["ed1", "ed2", "ed3"])
        pubMocks["findAndAddCover"].assert_called_once_with(testWork)
        pubMocks["parseEditions"].assert_called_once_with(["ed1", "ed2", "ed3"])

    def test_parseEditionToPublication_ReaderFlagFalse(self, testPubEls, mocker):
        testPub, _ = testPubEls

        testEdition = mocker.MagicMock(
            title="Test Title",
            sub_title="Test Sub",
            alt_titles=["alt1", "alt2"],
            identifiers=["id1", "id2", "id3"],
            publishers=[{"name": "Test Pub"}, {"name": "Other Pub"}],
            publication_date=mocker.MagicMock(year=2000),
            publication_place="Test Place",
            contributors=[{"name": "Test Contrib"}],
            summary="Test Description",
            languages=[None, {"iso_3": "tst"}, {"iso_3": "oth"}],
            date_created="testCreated",
            date_modified="testModified",
            items=[
                mocker.MagicMock(
                    links=[
                        mocker.MagicMock(
                            id="testID",
                            url="testURL",
                            media_type="testType",
                            flags={"reader": False},
                        )
                    ],
                    rights=[
                        mocker.MagicMock(
                            source="testSource",
                            license="testLicense",
                            rights_statement="testStatement",
                        )
                    ],
                )
            ],
            work=mocker.MagicMock(authors=[{"name": "Test Author"}]),
        )

        pubMocks = mocker.patch.multiple(
            Publication,
            setBestIdentifier=mocker.DEFAULT,
            setContributors=mocker.DEFAULT,
            addLink=mocker.DEFAULT,
            findAndAddCover=mocker.DEFAULT,
        )

        testPub.parseEditionToPublication(testEdition)

        testPub.metadata.addField.assert_has_calls(
            [
                mocker.call("title", "Test Title"),
                mocker.call("sortAs", "test title"),
                mocker.call("subtitle", "Test Sub"),
                mocker.call("alternate", ["alt1", "alt2"]),
                mocker.call("creator", "Test Author"),
                mocker.call("publisher", "Test Pub, Other Pub"),
                mocker.call("published", 2000),
                mocker.call("locationCreated", "Test Place"),
                mocker.call("description", "Test Description"),
                mocker.call("language", "tst,oth"),
                mocker.call("created", "testCreated"),
                mocker.call("modified", "testModified"),
                mocker.call(
                    "rights",
                    {
                        "source": "testSource",
                        "license": "testLicense",
                        "rightsStatement": "testStatement",
                    },
                ),
            ]
        )

        pubMocks["addLink"].assert_called_with(
            {
                "href": "testURL",
                "rel": "http://opds-spec.org/acquisition/open-access",
                "type": "testType",
                "identifier": "other",
            }
        )
        pubMocks["setContributors"].assert_called_once_with([{"name": "Test Contrib"}])
        pubMocks["setBestIdentifier"].assert_called_once_with(["id1", "id2", "id3"])
        pubMocks["findAndAddCover"].assert_called_once_with(testEdition)

    def test_parseEditionToPublication_ReaderFlagTrue(self, testPubEls, mocker):
        testPub, _ = testPubEls

        testEdition = mocker.MagicMock(
            title="Test Title",
            sub_title="Test Sub",
            alt_titles=["alt1", "alt2"],
            identifiers=["id1", "id2", "id3"],
            publishers=[{"name": "Test Pub"}, {"name": "Other Pub"}],
            publication_date=mocker.MagicMock(year=2000),
            publication_place="Test Place",
            contributors=[{"name": "Test Contrib"}],
            summary="Test Description",
            languages=[None, {"iso_3": "tst"}, {"iso_3": "oth"}],
            date_created="testCreated",
            date_modified="testModified",
            items=[
                mocker.MagicMock(
                    links=[
                        mocker.MagicMock(
                            id="testID",
                            url="testURL",
                            media_type="testType",
                            flags={"reader": True},
                        )
                    ],
                    rights=[
                        mocker.MagicMock(
                            source="testSource",
                            license="testLicense",
                            rights_statement="testStatement",
                        )
                    ],
                )
            ],
            work=mocker.MagicMock(authors=[{"name": "Test Author"}]),
        )

        pubMocks = mocker.patch.multiple(
            Publication,
            setBestIdentifier=mocker.DEFAULT,
            setContributors=mocker.DEFAULT,
            addLink=mocker.DEFAULT,
            findAndAddCover=mocker.DEFAULT,
        )

        testPub.parseEditionToPublication(testEdition)

        testPub.metadata.addField.assert_has_calls(
            [
                mocker.call("title", "Test Title"),
                mocker.call("sortAs", "test title"),
                mocker.call("subtitle", "Test Sub"),
                mocker.call("alternate", ["alt1", "alt2"]),
                mocker.call("creator", "Test Author"),
                mocker.call("publisher", "Test Pub, Other Pub"),
                mocker.call("published", 2000),
                mocker.call("locationCreated", "Test Place"),
                mocker.call("description", "Test Description"),
                mocker.call("language", "tst,oth"),
                mocker.call("created", "testCreated"),
                mocker.call("modified", "testModified"),
                mocker.call(
                    "rights",
                    {
                        "source": "testSource",
                        "license": "testLicense",
                        "rightsStatement": "testStatement",
                    },
                ),
            ]
        )

        pubMocks["addLink"].assert_called_with(
            {
                "href": "https://drb-qa.nypl.org/read/testID",
                "type": "testType",
                "rel": "http://opds-spec.org/acquisition/open-access",
                "identifier": "readable",
            }
        )
        pubMocks["setContributors"].assert_called_once_with([{"name": "Test Contrib"}])
        pubMocks["setBestIdentifier"].assert_called_once_with(["id1", "id2", "id3"])
        pubMocks["findAndAddCover"].assert_called_once_with(testEdition)

    def test_parseEditions(self, testPubEls, mocker):
        testPub, _ = testPubEls

        mockAdd = mocker.patch.object(Publication, "addEdition")

        mockPub = mocker.patch("api.opds2.publication.Publication")
        mockEdition = mocker.MagicMock()
        mockPub.return_value = mockEdition

        testPub.parseEditions(["testEdition"])

        mockPub.assert_called_once()
        mockEdition.parseEditionToPublication.assert_called_once_with("testEdition")
        mockAdd.assert_called_once_with(mockEdition)

    def test_setBestIdentifier(self, testPubEls, mocker):
        testPub, _ = testPubEls

        testPub.setBestIdentifier(
            [
                mocker.MagicMock(authority="test", identifier=1),
                mocker.MagicMock(authority="issn", identifier=2),
                mocker.MagicMock(authority="owi", identifier=3),
            ]
        )

        testPub.metadata.addField.assert_called_once_with("identifier", "urn:issn:2")

    def test_setContributors(self, testPubEls, mocker):
        testPub, _ = testPubEls

        testPub.setContributors(
            [
                {"name": "contrib1", "roles": ["editor", "illustrator"]},
                {"name": "contrib2", "roles": ["editor"]},
            ]
        )

        testPub.metadata.addField.assert_has_calls(
            [
                mocker.call("editor", "contrib1, contrib2"),
                mocker.call("illustrator", "contrib1"),
            ]
        )

    def test_setPreferredLink(self, testPubEls, mocker):
        testPub, _ = testPubEls

        mockAdd = mocker.patch.object(Publication, "addLink")

        testPub.setPreferredLink(
            [
                mocker.MagicMock(items=[]),
                mocker.MagicMock(
                    items=[
                        mocker.MagicMock(
                            links=[mocker.MagicMock(url="url1", media_type="testType")]
                        ),
                        mocker.MagicMock(
                            links=[mocker.MagicMock(url="url2", media_type="testType")]
                        ),
                    ]
                ),
            ]
        )

        mockAdd.assert_called_once_with(
            {
                "href": "url1",
                "type": "testType",
                "rel": "http://opds-spec.org/acquisition/open-access",
            }
        )

    def test_findAndAddCover_present(self, testPubEls, mocker):
        testPub, _ = testPubEls

        mocker.patch.dict("os.environ", {"DEFAULT_COVER_URL": "testDefaultCover"})

        def addImage(imageDict):
            testPub.images.append("testImage")

        mockAdd = mocker.patch.object(Publication, "addImage")
        mockAdd.side_effect = addImage

        testPub.findAndAddCover(
            mocker.MagicMock(
                editions=[
                    mocker.MagicMock(
                        links=[mocker.MagicMock(url="image1", media_type="image/jpeg")]
                    )
                ]
            )
        )

        mockAdd.assert_called_once_with({"href": "image1", "type": "image/jpeg"})

    def test_findAndAddCover_not_present(self, testPubEls, mocker):
        testPub, _ = testPubEls

        mocker.patch.dict("os.environ", {"DEFAULT_COVER_URL": "testDefaultCover"})
        mockAdd = mocker.patch.object(Publication, "addImage")

        testPub.findAndAddCover(
            mocker.MagicMock(
                links=[mocker.MagicMock(url="other1", media_type="text/html")]
            )
        )

        mockAdd.assert_called_once_with(
            {"href": "testDefaultCover", "type": "image/png"}
        )

    def test_dir(self, testPubEls):
        testPub, _ = testPubEls

        assert dir(testPub) == ["editions", "images", "links", "metadata", "type"]

    def test_iter_success(self, testIterableClass):
        testPub = Publication()

        testPub.metadata = "testMetadataBlock"
        testPub.images = [testIterableClass("img1"), testIterableClass("img2")]
        testPub.links = [testIterableClass("link1")]

        assert dict(testPub) == {
            "metadata": "testMetadataBlock",
            "images": [{"name": "img1"}, {"name": "img2"}],
            "links": [{"name": "link1"}],
            "editions": [],
            "type": "application/opds-publication+json",
        }

    def test_iter_error(self, testPubEls):
        testPub, _ = testPubEls

        testPub.images = []

        with pytest.raises(OPDS2PublicationException):
            dict(testPub)

    def test_repr(self, testPubEls, mocker):
        testPub, _ = testPubEls

        testPub.metadata = mocker.MagicMock(title="Test Title", author="Test Author")

        assert str(testPub) == "<Publication(title=Test Title, author=Test Author)>"
