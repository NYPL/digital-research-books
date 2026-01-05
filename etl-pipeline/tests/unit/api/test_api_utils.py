from hashlib import scrypt
import pytest
from random import shuffle
from flask import Flask, request

from api.utils import APIUtils
from datetime import datetime, timezone


class TestAPIUtils:
    @pytest.fixture
    def testApp(self):
        return Flask("test")

    @pytest.fixture
    def testAggregationResp(self):
        return {
            "editions": {
                "edition_filter_0": {
                    "languages": {
                        "buckets": [
                            {"key": "Lang1", "editions_per": {"doc_count": 1}},
                            {"key": "Lang2", "editions_per": {"doc_count": 3}},
                        ]
                    },
                    "formats": {
                        "buckets": [
                            {"key": "Format1", "editions_per": {"doc_count": 5}}
                        ]
                    },
                }
            }
        }

    @pytest.fixture
    def testHitObject(self, mocker):
        mockHits = [mocker.MagicMock(), mocker.MagicMock(), mocker.MagicMock()]
        mockHits[0].meta.sort = ("firstSort",)
        mockHits[-1].meta.sort = ("lastSort",)

        return mockHits

    @pytest.fixture
    def MockDBObject(self, mocker):
        class MockDB:
            def __init__(self, *args, **kwargs):
                self.attrs = []
                for key, value in kwargs.items():
                    self.attrs.append(key)
                    setattr(self, key, value)

            def __iter__(self):
                for attr in self.attrs:
                    value = getattr(self, attr)

                    if isinstance(value, mocker.MagicMock):
                        value = dict(value)
                    elif isinstance(value, list):
                        value = [dict(v) for v in value]

                    yield attr, value

        return MockDB

    @pytest.fixture
    def testRecord(self, MockDBObject):
        return MockDBObject(
            id="rec1",
            title="Test Record",
            spatial="Test Place",
            extent="Test Extent",
            abstract="Test Summary",
            table_of_contents="Test TOC",
            authors=["auth1", "auth2", "auth3"],
            contributors=["contrib1", "contrib2"],
            publisher=["pub1"],
            dates=["date1", "date2"],
            date_created="Test Created Date",
            date_modified="Test Modified Date",
            languages=["lang1"],
            identifiers=["id1", "id2", "id3"],
            has_part=["1|url1|", "2|url2|", "3|url3|"],
        )

    @pytest.fixture
    def testLink(self, MockDBObject):
        return MockDBObject(
            id="li1",
            media_type="application/epub+xml",
            url="testURI",
            flags={"test": True},
        )

    @pytest.fixture
    def testWebpubLink(self, MockDBObject):
        return MockDBObject(
            id="li2",
            media_type="application/webpub+json",
            url="testURI",
            flags={"test": True},
        )

    @pytest.fixture
    def testPDFLink(self, MockDBObject):
        return MockDBObject(
            id="li2", media_type="application/pdf", url="testURI", flags={"test": True}
        )

    @pytest.fixture
    def testRights(self, MockDBObject):
        return MockDBObject(
            id="ri1",
            source="test",
            license="testLicense",
            rights_statement="testStatement",
        )

    @pytest.fixture
    def testItem(self, MockDBObject, testLink, testRights):
        return MockDBObject(
            id="it1",
            links=[testLink],
            rights=[testRights],
            physical_location={"name": "test"},
            source="hathitrust",
        )

    @pytest.fixture
    def testWebpubItem(self, MockDBObject, testWebpubLink):
        return MockDBObject(
            id="it2",
            links=[testWebpubLink],
            rights=[],
            physical_location={},
            source="gutenberg",
        )

    @pytest.fixture
    def testPDFItem(self, MockDBObject, testPDFLink):
        return MockDBObject(
            id="it3",
            links=[testPDFLink],
            rights=[],
            physical_location={},
            source="gutenberg",
        )

    @pytest.fixture
    def testEmptyItem(self, MockDBObject):
        return MockDBObject(
            id="it4", links=[], rights=[], physical_location={}, source="gutenberg"
        )

    @pytest.fixture
    def testEdition(self, MockDBObject, testItem, mocker):
        return MockDBObject(
            id="ed1",
            work=mocker.MagicMock(uuid="uuid1"),
            publication_date=mocker.MagicMock(year=2000),
            items=[testItem],
            links=[mocker.MagicMock(id="co1", media_type="image/png", url="testCover")],
        )

    @pytest.fixture
    def testWork(self, MockDBObject, testEdition):
        return MockDBObject(
            uuid="testUUID",
            title="Test Title",
            editions=[testEdition],
            date_created=datetime.strptime("2022-05-12T10:00:41", "%Y-%m-%dT%H:%M:%S"),
            date_modified=datetime.strptime("2022-05-13T10:00:44", "%Y-%m-%dT%H:%M:%S"),
        )

    def test_normalizeQueryParams(self, mocker):
        mockParams = mocker.MagicMock()
        mockParams.to_dict.return_value = {"test1": 1, "test2": 2}

        testParams = APIUtils.normalizeQueryParams(mockParams)

        assert testParams == {"test1": 1, "test2": 2}

    def test_extractParamPairs(self):
        testPairs = APIUtils.extractParamPairs(
            "test", {"test": ["title:value", "bareValue"]}
        )

        assert testPairs[0] == ("title", "value")
        assert testPairs[1] == ("test", "bareValue")

    def test_extractParamPairs_comma_delimited(self):
        testPairs = APIUtils.extractParamPairs(
            "test", {"test": ["title:value,subject:value"]}
        )

        assert testPairs[0] == ("title", "value")
        assert testPairs[1] == ("subject", "value")

    def test_extractParamPairs_comma_delimited_quotes(self):
        testPairs = APIUtils.extractParamPairs(
            "test", {"test": ['title:value,author:"Test, Author",other']}
        )

        assert testPairs[0] == ("title", "value")
        assert testPairs[1] == ("author", '"Test, Author",other')

    def test_extractParamPairs_semantic_semicolon(self):
        testPairs = APIUtils.extractParamPairs(
            "test", {"test": ["title:A Book: A Title"]}
        )

        assert testPairs[0] == ("title", "A Book: A Title")

    def test_extractParamPairs_semicolon_no_field(self):
        testPairs = APIUtils.extractParamPairs("test", {"test": ["A Book: A Title"]})

        assert testPairs[0] == ("test", "A Book: A Title")

    def test_extractParamPairs_dangling_quotation(self):
        testPairs = APIUtils.extractParamPairs("test", {"test": ['"A Title']})

        assert testPairs[0] == ("test", "A Title")

    def test_extractParamPairs_dangling_quotation_multiple(self):
        testPairs = APIUtils.extractParamPairs(
            "test", {"test": ['"A Title",keyword:"other']}
        )

        assert testPairs[0] == ("test", '"A Title"')
        assert testPairs[1] == ("keyword", "other")

    def test_formatAggregationResult(self, testAggregationResp):
        testAggregations = APIUtils.formatAggregationResult(testAggregationResp)

        assert testAggregations["languages"][0] == {"value": "Lang1", "count": 1}
        assert testAggregations["languages"][1] == {"value": "Lang2", "count": 3}
        assert testAggregations["formats"][0] == {"value": "Format1", "count": 5}

    def test_formatFilters(self):
        testReadFormats = APIUtils.formatFilters({"filter": [("format", "readable")]})
        testDownloadFormats = APIUtils.formatFilters(
            {"filter": [("format", "downloadable")]}
        )
        testRequestFormats = APIUtils.formatFilters(
            {"filter": [("format", "requestable")]}
        )

        assert testReadFormats == [
            "application/epub+xml",
            "text/html",
            "application/webpub+json",
        ]
        assert testDownloadFormats == ["application/pdf", "application/epub+zip"]
        assert testRequestFormats == ["application/html+edd", "application/x.html+edd"]

    def test_formatPagingOptions_previous_null(self):
        testPagingOptions = APIUtils.formatPagingOptions(1, 10, 50)

        assert testPagingOptions == {
            "recordsPerPage": 10,
            "firstPage": 1,
            "previousPage": None,
            "currentPage": 1,
            "nextPage": 2,
            "lastPage": 5,
        }

    def test_formatPagingOptions_next_null(self):
        testPagingOptions = APIUtils.formatPagingOptions(6, 10, 55)

        assert testPagingOptions == {
            "recordsPerPage": 10,
            "firstPage": 1,
            "previousPage": 5,
            "currentPage": 6,
            "nextPage": None,
            "lastPage": 6,
        }

    def test_formatWorkOutput_single_work(self, mocker, testApp):
        mockFormat = mocker.patch.object(APIUtils, "formatWork")
        mockFormat.return_value = {
            "uuid": 1,
            "editions": [
                {"id": "ed1", "publication_date": None},
                {"id": "ed2", "publication_date": 2000},
                {"id": "ed3", "publication_date": 1900},
            ],
        }

        with testApp.test_request_context("/"):
            outWork = APIUtils.formatWorkOutput(
                "testWork", None, mocker.sentinel.dbClient, request=request
            )

        assert outWork["uuid"] == 1
        assert outWork["editions"][0]["id"] == "ed3"
        assert outWork["editions"][2]["id"] == "ed1"
        mockFormat.assert_called_once_with(
            "testWork",
            None,
            True,
            mocker.sentinel.dbClient,
            reader=None,
            request=request,
        )

    def test_formatWorkOutput_multiple_works(self, mocker, testApp):
        mockFormat = mocker.patch.object(APIUtils, "formatWork")
        mockFormat.side_effect = ["formattedWork1", "formattedWork2"]

        mockAddMeta = mocker.patch.object(APIUtils, "addWorkMeta")

        testWorks = [mocker.MagicMock(uuid="uuid1"), mocker.MagicMock(uuid="uuid2")]

        with testApp.test_request_context("/"):
            outWorks = APIUtils.formatWorkOutput(
                testWorks,
                [
                    ("uuid1", 1, "highlight1"),
                    ("uuid2", 2, "highlight2"),
                    ("uuid3", 3, "highlight3"),
                ],
                dbClient=mocker.sentinel.dbClient,
                request=request,
            )

        assert outWorks == ["formattedWork1", "formattedWork2"]
        mockFormat.assert_has_calls(
            [
                mocker.call(
                    testWorks[0],
                    1,
                    True,
                    mocker.sentinel.dbClient,
                    formats=None,
                    reader=None,
                    request=request,
                ),
                mocker.call(
                    testWorks[1],
                    2,
                    True,
                    mocker.sentinel.dbClient,
                    formats=None,
                    reader=None,
                    request=request,
                ),
            ]
        )

        mockAddMeta.assert_has_calls(
            [
                mocker.call("formattedWork1", highlights="highlight1"),
                mocker.call("formattedWork2", highlights="highlight2"),
            ]
        )

    def test_formatWork_showAll(self, testWork, mocker, testApp):
        mockFormatEdition = mocker.patch.object(APIUtils, "formatEdition")
        mockFormatEdition.return_value = {
            "edition_id": "ed1",
            "items": [{"item1": "foo"}],
        }
        testWork.id = "testID"

        mockDB = mocker.MagicMock()
        mockDBClient = mocker.patch("api.blueprints.drbWork.DBClient")
        mockDBClient.return_value = mockDB

        with testApp.test_request_context("/"):
            testWorkDict = APIUtils.formatWork(
                testWork, ["ed1"], True, dbClient=mockDBClient, request=request
            )

            assert testWorkDict["uuid"] == "testUUID"
            assert testWorkDict["title"] == "Test Title"
            assert testWorkDict["editions"][0]["edition_id"] == "ed1"
            assert testWorkDict["editions"][0]["items"][0] == {"item1": "foo"}
            assert testWorkDict["edition_count"] == 1
            assert testWorkDict["date_created"] == "2022-05-12T10:00:41"
            assert testWorkDict["date_modified"] == "2022-05-13T10:00:44"
            mockFormatEdition.assert_called_once()

    def test_formatWork_showAll_false(self, testWork, mocker, testApp):
        mockDB = mocker.MagicMock()
        mockDBClient = mocker.patch("api.blueprints.drbWork.DBClient")
        mockDBClient.return_value = mockDB

        with testApp.test_request_context("/"):
            testWork.id = "testID"
            mockFormatEdition = mocker.patch.object(APIUtils, "formatEdition")
            mockFormatEdition.return_value = {
                "edition_id": "ed1",
                "items": [{"item1": "foo"}],
            }
            testWorkDict = APIUtils.formatWork(
                testWork, ["ed1"], False, dbClient=mockDBClient, request=request
            )

            assert testWorkDict["uuid"] == "testUUID"
            assert testWorkDict["title"] == "Test Title"
            assert len(testWorkDict["editions"]) == 1
            assert testWorkDict["edition_count"] == 1
            assert testWorkDict["date_created"] == "2022-05-12T10:00:41"
            assert testWorkDict["date_modified"] == "2022-05-13T10:00:44"

    def test_formatWork_blocked_edition(self, testWork, mocker, testApp):
        mockDB = mocker.MagicMock()
        mockDBClient = mocker.patch("api.blueprints.drbWork.DBClient")
        mockDBClient.return_value = mockDB

        with testApp.test_request_context("/"):
            testWork.id = "testID"
            testWork.editions[0].items = []
            testWorkDict = APIUtils.formatWork(
                testWork, ["ed2"], True, dbClient=mockDBClient, request=request
            )

            assert testWorkDict["uuid"] == "testUUID"
            assert testWorkDict["title"] == "Test Title"
            assert len(testWorkDict["editions"]) == 0
            assert testWorkDict["edition_count"] == 1
            assert testWorkDict["date_created"] == "2022-05-12T10:00:41"
            assert testWorkDict["date_modified"] == "2022-05-13T10:00:44"

    def test_formatWork_ordered_editions(self, testWork, mocker, testApp):
        testWork.editions = [mocker.MagicMock(id=1), mocker.MagicMock(id=2)]
        testWork.id = "testID"

        mockDB = mocker.MagicMock()
        mockDBClient = mocker.patch("api.blueprints.drbWork.DBClient")
        mockDBClient.return_value = mockDB

        mockFormatEdition = mocker.patch.object(APIUtils, "formatEdition")
        mockFormatEdition.side_effect = [
            {"edition_id": "ed1", "items": [{"it1": "item"}]},
            {"edition_id": "ed2", "items": [{"it2": "item"}]},
        ]

        with testApp.test_request_context("/"):
            testWorkDict = APIUtils.formatWork(
                testWork, [2, 1], True, dbClient=mockDBClient, request=request
            )

            assert testWorkDict["editions"][0]["edition_id"] == "ed2"
            assert testWorkDict["editions"][1]["edition_id"] == "ed1"

    def test_formatEditionOutput(self, mocker, testApp):
        mockFormatEdition = mocker.patch.object(APIUtils, "formatEdition")
        mockFormatEdition.return_value = {"testEdition": "test"}

        mockDB = mocker.MagicMock()
        mockDBClient = mocker.patch("api.blueprints.drbEdition.DBClient")
        mockDBClient.return_value = mockDB

        mockEdition = mocker.MagicMock(dcdw_uuids="testUUID")

        mockDB.fetchSingleEdition.return_value = mockEdition

        with testApp.test_request_context("/"):
            assert APIUtils.formatEditionOutput(
                mockEdition,
                records="testRecords",
                dbClient=mockDBClient,
                request=request,
                showAll=True,
            ) == {"testEdition": "test"}

        mockFormatEdition.assert_called_once_with(
            mockEdition,
            mockEdition.work.title,
            mockEdition.work.authors,
            [],
            "testRecords",
            None,
            showAll=True,
            reader=None,
        )

    def test_formatEdition_no_records(self, testEdition):
        formattedEdition = APIUtils.formatEdition(testEdition)

        assert formattedEdition["work_uuid"] == "uuid1"
        assert formattedEdition["edition_id"] == "ed1"
        assert formattedEdition["publication_date"] == 2000
        assert formattedEdition["links"][0]["link_id"] == "co1"
        assert formattedEdition["links"][0]["mediaType"] == "image/png"
        assert formattedEdition["links"][0]["url"] == "testCover"
        assert formattedEdition["items"][0]["item_id"] == "it1"
        assert formattedEdition["items"][0]["location"] == "test"
        assert formattedEdition["items"][0]["links"][0]["link_id"] == "li1"
        assert (
            formattedEdition["items"][0]["links"][0]["mediaType"]
            == "application/epub+xml"
        )
        assert formattedEdition["items"][0]["links"][0]["url"] == "testURI"
        assert formattedEdition["items"][0]["links"][0]["flags"]["test"] is True
        assert formattedEdition["items"][0]["rights"][0]["source"] == "test"
        assert formattedEdition["items"][0]["rights"][0]["license"] == "testLicense"
        assert (
            formattedEdition["items"][0]["rights"][0]["rightsStatement"]
            == "testStatement"
        )
        assert formattedEdition.get("instances", None) is None

    def test_formatEdition_w_records(self, testEdition, mocker):
        mockRecFormat = mocker.patch.object(APIUtils, "formatRecord")
        mockRecFormat.side_effect = [
            {"id": 1, "items": []},
            {"id": 2, "items": ["it1"]},
        ]

        formattedEdition = APIUtils.formatEdition(
            testEdition,
            editionWorkTitle=None,
            editionWorkAuthors=None,
            records=["rec1", "rec2"],
            showAll=False,
        )

        assert len(formattedEdition["instances"]) == 1
        assert formattedEdition["instances"][0]["id"] == 2
        assert formattedEdition.get("items", None) is None

        testItemDict = {
            "id": "it1",
            "links": [
                {
                    "link_id": "li1",
                    "mediaType": "application/epub+xml",
                    "url": "testURI",
                    "flags": {"test": True},
                }
            ],
            "rights": [
                {
                    "source": "test",
                    "license": "testLicense",
                    "rightsStatement": "testStatement",
                }
            ],
            "physical_location": {"name": "test"},
            "item_id": "it1",
            "location": "test",
            "source": "hathitrust",
        }

        mockRecFormat.assert_has_calls(
            [
                mocker.call("rec1", {"testURI": testItemDict}),
                mocker.call("rec2", {"testURI": testItemDict}),
            ]
        )

    def test_formatEdition_v1_reader_flag(self, testEdition, testWebpubItem):
        testEdition.items.append(testWebpubItem)

        formattedEdition = APIUtils.formatEdition(testEdition, reader="v1")

        assert len(formattedEdition["items"]) == 2
        assert formattedEdition["items"][0]["links"][0]["flags"]["reader"] is False
        assert formattedEdition["items"][1]["item_id"] == "it1"
        assert (
            formattedEdition["items"][1]["links"][0]["mediaType"]
            == "application/epub+xml"
        )

    def test_formatEdition_v2_reader_flag(
        self, testEdition, testWebpubItem, testPDFItem
    ):
        testEdition.items.append(testPDFItem)
        testEdition.items.append(testWebpubItem)

        formattedEdition = APIUtils.formatEdition(testEdition, reader="v2")

        assert len(formattedEdition["items"]) == 3
        assert formattedEdition["items"][0]["item_id"] == "it2"
        assert (
            formattedEdition["items"][0]["links"][0]["mediaType"]
            == "application/webpub+json"
        )
        assert (
            formattedEdition["items"][1]["links"][0]["mediaType"] == "application/pdf"
        )
        assert formattedEdition["items"][2]["links"][0]["flags"]["reader"] is False

    def test_formatEdition_v2_reader_flag_2(
        self, testEdition, testWebpubItem, testPDFItem, testEmptyItem
    ):
        testEdition.items.append(testEmptyItem)
        testEdition.items.append(testWebpubItem)

        formattedEdition = APIUtils.formatEdition(testEdition, reader="v2")

        assert len(formattedEdition["items"]) == 3
        assert formattedEdition["items"][0]["item_id"] == "it2"
        assert (
            formattedEdition["items"][0]["links"][0]["mediaType"]
            == "application/webpub+json"
        )
        assert formattedEdition["items"][1]["links"] == []
        assert formattedEdition["items"][2]["links"][0]["flags"]["reader"] is False

    def test_formatRecord(self, testRecord, mocker):
        testLinkItems = {
            "url1": {"item_id": 1, "url": "url1"},
            "url2": {"item_id": 2, "url": "url2"},
        }

        mockFormatPipe = mocker.patch.object(APIUtils, "formatPipeDelimitedData")
        mockFormatPipe.side_effect = [
            "testAuthors",
            "testContribs",
            "testPublishers",
            "testDates",
            "testLangs",
            "testIDs",
        ]

        testFormatted = APIUtils.formatRecord(testRecord, testLinkItems)

        assert testFormatted["instance_id"] == "rec1"
        assert testFormatted["title"] == "Test Record"
        assert testFormatted["publication_place"] == "Test Place"
        assert testFormatted["extent"] == "Test Extent"
        assert testFormatted["summary"] == "Test Summary"
        assert testFormatted["table_of_contents"] == "Test TOC"
        assert testFormatted["authors"] == "testAuthors"
        assert testFormatted["contributors"] == "testContribs"
        assert testFormatted["publishers"] == "testPublishers"
        assert testFormatted["dates"] == "testDates"
        assert testFormatted["languages"] == "testLangs"
        assert testFormatted["identifiers"] == "testIDs"
        assert testFormatted["items"] == [
            {"item_id": 1, "url": "url1"},
            {"item_id": 2, "url": "url2"},
        ]

    def test_formatLinkOutput(self, testLink, testWork, testEdition, testItem, testApp):
        testEdition.work = testWork
        testItem.edition = testEdition
        testLink.items = [testItem]

        with testApp.test_request_context("/"):
            testLink = APIUtils.formatLinkOutput(testLink, request)

        assert testLink["link_id"] == "li1"
        assert testLink["work"]["uuid"] == "testUUID"
        assert testLink["work"]["editions"][0]["edition_id"] == "ed1"
        assert testLink["work"]["editions"][0]["items"][0]["item_id"] == "it1"

    def test_formatLanguages_no_counts(self, mocker):
        mockAggs = mocker.MagicMock()
        mockAggs.languages.languages.buckets = [
            mocker.MagicMock(key="bLang"),
            mocker.MagicMock(key="aLang"),
        ]

        assert APIUtils.formatLanguages(mockAggs) == [
            {"language": "aLang"},
            {"language": "bLang"},
        ]

    def test_formatLanguages_counts(self, mocker):
        mockAggs = mocker.MagicMock()
        mockAggs.languages.languages.buckets = [
            mocker.MagicMock(key="bLang", work_totals=mocker.MagicMock(doc_count=10)),
            mocker.MagicMock(key="cLang", work_totals=mocker.MagicMock(doc_count=30)),
            mocker.MagicMock(key="aLang", work_totals=mocker.MagicMock(doc_count=20)),
        ]

        assert APIUtils.formatLanguages(mockAggs, True) == [
            {"language": "cLang", "work_total": 30},
            {"language": "aLang", "work_total": 20},
            {"language": "bLang", "work_total": 10},
        ]

    def test_formatTotals(self):
        assert APIUtils.formatTotals([("test", 3), ("other", 6)]) == {
            "test": 3,
            "other": 6,
        }

    def test_flatten_already_flat(self):
        flatArray = [i for i in APIUtils.flatten([1, 2, 3, 4, 5])]

        assert flatArray == [1, 2, 3, 4, 5]

    def test_flatten_nested(self):
        flatArray = [i for i in APIUtils.flatten([[1, 2], 3, [4, [5]]])]

        assert flatArray == [1, 2, 3, 4, 5]

    def test_formatResponseObject(self, mocker, testApp):
        with testApp.test_request_context("/"):
            mockDatetime = mocker.patch("api.utils.datetime")
            mockDatetime.now.return_value.replace.return_value = "presentTimestamp"

            testResponse = APIUtils.formatResponseObject(
                200, "test", {"test": "test data"}
            )

            assert testResponse[0].json == {
                "status": 200,
                "timestamp": "presentTimestamp",
                "responseType": "test",
                "data": {"test": "test data"},
            }
            assert testResponse[1] == 200
            mockDatetime.now.assert_called_once

    def test_formatPipeDelimitedData_string(self):
        assert APIUtils.formatPipeDelimitedData("test|object", ["one", "two"]) == {
            "one": "test",
            "two": "object",
        }

    def test_formatPipeDelimitedData_list(self):
        assert APIUtils.formatPipeDelimitedData(
            ["test|object", None, "another|thing"], ["one", "two"]
        ) == [{"one": "test", "two": "object"}, {"one": "another", "two": "thing"}]

    def test_formatPipeDelimitedData_none(self):
        assert APIUtils.formatPipeDelimitedData(None, ["one", "two"]) is None

    def test_validatePassword_success(self):
        testHash = scrypt(b"testPswd", salt=b"testSalt", n=2**14, r=8, p=1)

        assert APIUtils.validatePassword("testPswd", testHash, b"testSalt") is True

    def test_validatePassword_error(self):
        testHash = scrypt(b"testPswd", salt=b"testSalt", n=2**14, r=8, p=1)

        assert APIUtils.validatePassword("testError", testHash, b"testSalt") is False

    def test_addWorkMeta(self):
        testWork = {}

        APIUtils.addWorkMeta(testWork, field1="value1", field2=["value2"])

        assert testWork["_meta"]["field1"] == "value1"
        assert testWork["_meta"]["field2"] == ["value2"]

    def test_sortByMediaType(self):
        testList = [
            {"id": 4, "mediaType": "text/html"},
            {"id": 3, "mediaType": "application/epub+xml"},
            {"id": 5, "mediaType": "application/html+edd"},
            {"id": 3, "mediaType": "application/epub+zip"},
            {"id": 1, "mediaType": "application/webpub+json"},
            {"id": 2, "mediaType": "application/pdf"},
        ]

        shuffle(testList)
        testList.sort(key=APIUtils.sortByMediaType)
        assert [i["id"] for i in testList] == [1, 2, 3, 3, 4, 5]

        shuffle(testList)
        testList.sort(key=APIUtils.sortByMediaType)
        assert [i["id"] for i in testList] == [1, 2, 3, 3, 4, 5]

    def test_getPresignedUrlFromObjectUrl(self, mocker):
        mockGenerateUrl = mocker.patch.object(APIUtils, "generate_presigned_url")
        mockGenerateUrl.return_value = "https://example.com/mypresignedurl"
        assert (
            APIUtils.getPresignedUrlFromObjectUrl(
                {"Some Client"},
                "https://doc-example-bucket1.s3.us-west-2.amazonaws.com/puppy.png",
            )
            == "https://example.com/mypresignedurl"
        )
        mockGenerateUrl.assert_called_once_with(
            {"Some Client"},
            "get_object",
            {"Bucket": "doc-example-bucket1", "Key": "puppy.png"},
            30000,
        )

    def test_getPresignedUrlFromNons3Url(self):
        with pytest.raises(ValueError):
            APIUtils.getPresignedUrlFromObjectUrl(
                {"Some Client"}, "https://example.com"
            )

    def test_ReplaceWithPrivateLink(self, testApp):
        with testApp.test_request_context("/", base_url="http://localhost:5000"):
            testLoginLinkObj = {
                "link_id": "12345",
                "media_type": "application/pdf",
                "url": "https://doc-example-bucket1.s3.us-west-2.amazonaws.com/puppy.pdf",
                "flags": {"nypl_login": True},
            }
            assert APIUtils.replacePrivateLinkUrl(testLoginLinkObj, request) == {
                "link_id": "12345",
                "media_type": "application/pdf",
                "url": "localhost:5000/fulfill/12345",
                "flags": {"nypl_login": True},
            }

    def test_noLinkReplacement(self, testApp):
        testElectronicDeliveryLink = {
            "link_id": "6789",
            "media_type": "application/html+edd",
            "flags": {
                "catalog": False,
                "download": False,
                "edd": True,
                "reader": False,
            },
        }
        with testApp.test_request_context("/"):
            assert (
                APIUtils.replacePrivateLinkUrl(testElectronicDeliveryLink, request)
                == testElectronicDeliveryLink
            )
