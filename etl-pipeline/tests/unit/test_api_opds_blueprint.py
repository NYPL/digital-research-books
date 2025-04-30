from flask import Flask
import pytest

from api.blueprints.drbOPDS2 import (
    opdsRoot,
    newPublications,
    opdsSearch,
    fetchPublication,
    constructBaseFeed,
    addPublications,
    createPublicationObject,
    addFacets,
)
from api.utils import APIUtils
from api.opdsUtils import OPDSUtils


class TestOPDSBlueprint:
    @pytest.fixture
    def mockUtils(self, mocker):
        return mocker.patch.multiple(
            APIUtils,
            normalizeQueryParams=mocker.DEFAULT,
            extractParamPairs=mocker.DEFAULT,
            formatAggregationResult=mocker.DEFAULT,
            formatPagingOptions=mocker.DEFAULT,
            formatWorkOutput=mocker.DEFAULT,
            formatOPDS2Object=mocker.DEFAULT,
            formatResponseObject=mocker.DEFAULT,
        )

    @pytest.fixture
    def opdsMocks(self, mocker):
        return mocker.patch.multiple(
            "api.blueprints.drbOPDS2",
            constructBaseFeed=mocker.DEFAULT,
            addFacets=mocker.DEFAULT,
            addPublications=mocker.DEFAULT,
            createPublicationObject=mocker.DEFAULT,
        )

    @pytest.fixture
    def FakeHit(self, mocker):
        class FakeHit:
            def __init__(self, uuid, edition_ids):
                self.uuid = uuid
                editions = []
                for edition_id in edition_ids:
                    ed = mocker.MagicMock()
                    ed.edition_id = edition_id
                    editions.append(ed)
                mockMeta = mocker.MagicMock()
                mockMeta.inner_hits.editions.hits = editions
                mockMeta.highlight = {"field": ["highlight_{}".format(uuid)]}
                self.meta = mockMeta

        return FakeHit

    @pytest.fixture
    def mockHits(self, FakeHit, mocker):
        class FakeHits:
            def __init__(self):
                self.total = mocker.MagicMock(value=5)
                self.hits = [
                    FakeHit("uuid1", ["ed1", "ed2"]),
                    FakeHit("uuid2", ["ed3"]),
                    FakeHit("uuid3", ["ed4", "ed5", "ed6"]),
                    FakeHit("uuid4", ["ed7"]),
                    FakeHit("uuid5", ["ed8"]),
                ]

            def __iter__(self):
                for h in self.hits:
                    yield h

        return FakeHits()

    def test_opdsRootEndpoint(self, mockUtils, mocker):
        testFlask = Flask("test")

        mockBase = mocker.patch("api.blueprints.drbOPDS2.constructBaseFeed")
        mockBase.return_value = "testRootFeed"

        mockUtils["formatOPDS2Object"].return_value = "testOPDSResponse"

        with testFlask.test_request_context("/"):
            assert opdsRoot() == "testOPDSResponse"

            mockBase.assert_called_once_with("/", "Digital Research Books Home")
            mockUtils["formatOPDS2Object"].assert_called_once_with(200, "testRootFeed")

    def test_newPublicationsEndpoint(self, mockUtils, opdsMocks, mocker):
        testFlask = Flask("test")
        testFlask.config["DB_CLIENT"] = "testDBClient"

        mockUtils["normalizeQueryParams"].return_value = {}

        mockDB = mocker.MagicMock()
        mockDBClient = mocker.patch("api.blueprints.drbOPDS2.DBClient")
        mockDBClient.return_value = mockDB
        mockDB.fetchNewWorks.return_value = (3, ["pub1", "pub2", "pub3"])

        opdsMocks["constructBaseFeed"].return_value = "testBaseFeed"

        mockUtils["formatOPDS2Object"].return_value = "testOPDSResponse"

        mockAddPaging = mocker.patch.object(OPDSUtils, "addPagingOptions")

        with testFlask.test_request_context("/new"):
            assert newPublications() == "testOPDSResponse"

            mockUtils["normalizeQueryParams"].assert_called_once_with({})
            mockDBClient.assert_called_once_with("testDBClient")
            opdsMocks["constructBaseFeed"].assert_called_once_with(
                "/new?", "New Publications: Digital Research Books", grouped=True
            )
            mockDB.fetchNewWorks.assert_called_once_with(page=0, size=25)
            mockAddPaging.assert_called_once_with(
                "testBaseFeed", "/new?", 3, page=1, perPage=25
            )
            opdsMocks["addPublications"].assert_called_once_with(
                "testBaseFeed", ["pub1", "pub2", "pub3"], grouped=True
            )
            mockUtils["formatOPDS2Object"].assert_called_once_with(200, "testBaseFeed")

    def test_opdsSearch(self, mockUtils, opdsMocks, mockHits, mocker):
        flaskApp = Flask("test")
        flaskApp.config["DB_CLIENT"] = "testDBClient"
        flaskApp.config["REDIS_CLIENT"] = "testRedisClient"

        mockES = mocker.MagicMock()
        mockESClient = mocker.patch("api.blueprints.drbOPDS2.ElasticClient")
        mockESClient.return_value = mockES

        mockDB = mocker.MagicMock()
        mockDBClient = mocker.patch("api.blueprints.drbOPDS2.DBClient")
        mockDBClient.return_value = mockDB

        mockUtils["normalizeQueryParams"].return_value = {
            "showAll": ["testShow"],
            "title": ["testTitle"],
        }
        mockUtils["extractParamPairs"].return_value = ["testFilter"]

        mockAgg = mocker.MagicMock()
        mockAgg.to_dict.return_value = {"aggs": []}
        mockResponse = mocker.MagicMock(hits=mockHits, aggregations=mockAgg)
        mockES.searchQuery.return_value = mockResponse

        mockDB.fetchSearchedWorks.return_value = [
            "work1",
            "work2",
            "work3",
            "work4",
            "work5",
        ]

        opdsMocks["constructBaseFeed"].return_value = "testBaseFeed"

        mockUtils["formatOPDS2Object"].return_value = "mockOPDSResponse"

        mockAddPaging = mocker.patch.object(OPDSUtils, "addPagingOptions")

        with flaskApp.test_request_context("/search"):
            assert opdsSearch() == "mockOPDSResponse"

            mockESClient.assert_called_once_with("testRedisClient")
            mockDBClient.assert_called_once_with("testDBClient")

            mockUtils["normalizeQueryParams"].assert_called_once()
            mockES.searchQuery.assert_called_once_with(
                {
                    "query": [("title", "testTitle")],
                    "sort": [],
                    "filter": ["testFilter", ("showAll", "testShow")],
                },
                page=0,
                perPage=25,
            )
            mockDB.fetchSearchedWorks.assert_called_once_with(
                [
                    ("uuid1", ["ed1", "ed2"]),
                    ("uuid2", ["ed3"]),
                    ("uuid3", ["ed4", "ed5", "ed6"]),
                    ("uuid4", ["ed7"]),
                    ("uuid5", ["ed8"]),
                ]
            )

            opdsMocks["constructBaseFeed"].assert_called_once_with(
                "/search?", "Search Results", grouped=True
            )
            mockAddPaging.assert_called_once_with(
                "testBaseFeed", "/search?", 5, page=1, perPage=25
            )
            opdsMocks["addFacets"].assert_called_once_with(
                "testBaseFeed", "/search?", {"aggs": []}
            )
            opdsMocks["addPublications"].assert_called_once_with(
                "testBaseFeed",
                ["work1", "work2", "work3", "work4", "work5"],
                grouped=True,
                highlights={
                    "uuid1": {"field": ["highlight_uuid1"]},
                    "uuid2": {"field": ["highlight_uuid2"]},
                    "uuid3": {"field": ["highlight_uuid3"]},
                    "uuid4": {"field": ["highlight_uuid4"]},
                    "uuid5": {"field": ["highlight_uuid5"]},
                },
            )
            mockUtils["formatOPDS2Object"].assert_called_once_with(200, "testBaseFeed")

    def test_singlePublicationEndpoint_success(self, mockUtils, opdsMocks, mocker):
        testFlask = Flask("test")
        testFlask.config["DB_CLIENT"] = "testDBClient"

        mockDB = mocker.MagicMock()
        mockDBClient = mocker.patch("api.blueprints.drbOPDS2.DBClient")
        mockDBClient.return_value = mockDB
        mockDB.fetchSingleWork.return_value = "testWorkRecord"

        mockPublication = mocker.MagicMock()
        opdsMocks["createPublicationObject"].return_value = mockPublication

        mockUtils["formatOPDS2Object"].return_value = "testOPDSResponse"

        with testFlask.test_request_context("/publication/testUUID"):
            assert fetchPublication("testUUID") == "testOPDSResponse"

            mockDBClient.assert_called_once_with("testDBClient")
            mockDB.fetchSingleWork.assert_called_once_with("testUUID")

            opdsMocks["createPublicationObject"].assert_called_once_with(
                "testWorkRecord", searchResult=False
            )

            mockPublication.addLink.assert_called_once_with(
                {
                    "rel": "search",
                    "href": "/opds/search{?query,title,subject,author}",
                    "type": "application/opds+json",
                    "templated": True,
                }
            )

            mockUtils["formatOPDS2Object"].assert_called_once_with(200, mockPublication)

    def test_singlePublicationEndpoint_missing(self, mockUtils, opdsMocks, mocker):
        testFlask = Flask("test")
        testFlask.config["DB_CLIENT"] = "testDBClient"

        mockDB = mocker.MagicMock()
        mockDBClient = mocker.patch("api.blueprints.drbOPDS2.DBClient")
        mockDBClient.return_value = mockDB
        mockDB.fetchSingleWork.return_value = None

        mockUtils["formatResponseObject"].return_value = "testErrorResponse"

        with testFlask.test_request_context("/publication/testUUID"):
            assert fetchPublication("testUUID") == "testErrorResponse"

            mockDBClient.assert_called_once_with("testDBClient")
            mockDB.fetchSingleWork.assert_called_once_with("testUUID")

            mockUtils["formatResponseObject"].assert_called_once_with(
                404,
                "opdsPublication",
                {"message": "Unable to find work for uuid testUUID"},
            )

    def test_constructBaseFeed_root(self, mocker):
        mockFeed = mocker.MagicMock()
        mockFeedCon = mocker.patch("api.blueprints.drbOPDS2.Feed")
        mockFeedCon.return_value = mockFeed

        mockMeta = mocker.MagicMock()
        mockMetaCon = mocker.patch("api.blueprints.drbOPDS2.Metadata")
        mockMetaCon.return_value = mockMeta

        mockLinkCon = mocker.patch("api.blueprints.drbOPDS2.Link")
        mockLinkCon.side_effect = ["selfLink", "searchLink", "altLink"]

        mockNavCon = mocker.patch("api.blueprints.drbOPDS2.Navigation")
        mockNavCon.side_effect = ["currentNav", "newNav"]

        mockGroup = mocker.MagicMock()
        mockGroupCon = mocker.patch("api.blueprints.drbOPDS2.Group")
        mockGroupCon.return_value = mockGroup

        assert constructBaseFeed("/opds/", "Test Feed", grouped=True) == mockFeed

        mockFeedCon.assert_called_once()
        mockMetaCon.assert_called_once_with(title="Test Feed")
        mockFeed.addMetadata.assert_called_once_with(mockMeta)

        assert mockLinkCon.call_count == 3
        mockFeed.addLinks.assert_called_with(["selfLink", "searchLink", "altLink"])

        assert mockNavCon.call_count == 2

        assert mockGroupCon.called_once_with(metadata={"title": "Main Menu"})
        mockGroup.addNavigations.assert_called_once_with(["currentNav", "newNav"])
        mockFeed.addGroup.assert_called_once_with(mockGroup)

    def test_constructBaseFeed_new_non_grouped(self, mocker):
        mockFeed = mocker.MagicMock()
        mockFeedCon = mocker.patch("api.blueprints.drbOPDS2.Feed")
        mockFeedCon.return_value = mockFeed

        mockMeta = mocker.MagicMock()
        mockMetaCon = mocker.patch("api.blueprints.drbOPDS2.Metadata")
        mockMetaCon.return_value = mockMeta

        mockLinkCon = mocker.patch("api.blueprints.drbOPDS2.Link")
        mockLinkCon.side_effect = ["selfLink", "searchLink", "altLink"]

        mockNavCon = mocker.patch("api.blueprints.drbOPDS2.Navigation")
        mockNavCon.side_effect = ["currentNav", "newNav"]

        assert constructBaseFeed("/opds/new/", "Test Feed") == mockFeed

        mockFeedCon.assert_called_once()
        mockMetaCon.assert_called_once_with(title="Test Feed")
        mockFeed.addMetadata.assert_called_once_with(mockMeta)

        assert mockLinkCon.call_count == 3
        mockFeed.addLinks.assert_called_with(["selfLink", "searchLink", "altLink"])

        assert mockNavCon.call_count == 2

        mockFeed.addNavigations.assert_called_once_with(["currentNav", "newNav"])

    def test_addPublications_grouped(self, opdsMocks, mocker):
        opdsMocks["createPublicationObject"].side_effect = ["pub1", "pub2", "pub3"]
        mockFeed = mocker.MagicMock()

        mockGroup = mocker.MagicMock()
        mockGroupCon = mocker.patch("api.blueprints.drbOPDS2.Group")
        mockGroupCon.return_value = mockGroup

        mockPubs = [
            mocker.MagicMock(uuid=1),
            mocker.MagicMock(uuid=2),
            mocker.MagicMock(uuid=3),
        ]

        addPublications(
            mockFeed,
            mockPubs,
            grouped=True,
            highlights={"1": "highlights1", "2": "highlights2", "3": "highlights3"},
        )

        opdsMocks["createPublicationObject"].assert_has_calls(
            [
                mocker.call(
                    mockPubs[i], _meta={"highlights": "highlights{}".format(i + 1)}
                )
                for i in range(3)
            ]
        )
        mockGroupCon.assert_called_once_with(metadata={"title": "Publications"})
        mockGroup.addPublications.assert_called_once_with(["pub1", "pub2", "pub3"])
        mockFeed.addGroup.assert_called_once_with(mockGroup)

    def test_addPublications_not_grouped(self, opdsMocks, mocker):
        opdsMocks["createPublicationObject"].side_effect = ["pub1", "pub2", "pub3"]
        mockFeed = mocker.MagicMock()

        mockPubs = [
            mocker.MagicMock(uuid=1),
            mocker.MagicMock(uuid=2),
            mocker.MagicMock(uuid=3),
        ]

        addPublications(
            mockFeed,
            mockPubs,
            highlights={"1": "highlights1", "2": "highlights2", "3": "highlights3"},
        )

        opdsMocks["createPublicationObject"].assert_has_calls(
            [
                mocker.call(
                    mockPubs[i], _meta={"highlights": "highlights{}".format(i + 1)}
                )
                for i in range(3)
            ]
        )
        mockFeed.addPublications.assert_called_once_with(["pub1", "pub2", "pub3"])

    def test_createPublicationObject(self, mocker):
        mockPub = mocker.MagicMock()
        mockPubCon = mocker.patch("api.blueprints.drbOPDS2.Publication")
        mockPubCon.return_value = mockPub

        assert createPublicationObject("testPub") == mockPub

        mockPubCon.assert_called_once()
        mockPub.parseWorkToPublication.assert_called_once_with(
            "testPub", searchResult=True
        )

    def test_addFacets(self, mockUtils, mocker):
        mockFeed = mocker.MagicMock()

        mockUtils["formatAggregationResult"].return_value = {
            "tests": [
                {"value": "facet1", "count": 3},
                {"value": "facet2", "count": 5},
                {"value": "facet3", "count": 1},
            ]
        }

        mockFacet = mocker.MagicMock()
        mockFacetCon = mocker.patch("api.blueprints.drbOPDS2.Facet")
        mockFacetCon.side_effect = [mockFacet, mockFacet]

        addFacets(mockFeed, "/test", "testFacets")

        mockUtils["formatAggregationResult"].assert_called_once_with("testFacets")
        mockFacetCon.assert_has_calls(
            [
                mocker.call(metadata={"title": "tests"}),
                mocker.call(
                    metadata={"title": "Show All Editions"},
                    links=[
                        {
                            "href": "/test&showAll=true",
                            "type": "application/opds+json",
                            "title": "True",
                        },
                        {
                            "href": "/test&showAll=false",
                            "type": "application/opds+json",
                            "title": "False",
                        },
                    ],
                ),
            ]
        )
        mockFacet.addLinks.assert_called_once_with(
            [
                {
                    "href": "/test&filter=test:facet1",
                    "type": "application/opds+json",
                    "title": "facet1",
                    "properties": {"numberOfItems": 3},
                },
                {
                    "href": "/test&filter=test:facet2",
                    "type": "application/opds+json",
                    "title": "facet2",
                    "properties": {"numberOfItems": 5},
                },
                {
                    "href": "/test&filter=test:facet3",
                    "type": "application/opds+json",
                    "title": "facet3",
                    "properties": {"numberOfItems": 1},
                },
            ]
        )
        mockFeed.addFacets.assert_called_once_with([mockFacet, mockFacet])
