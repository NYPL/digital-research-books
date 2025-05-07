import pytest

from elasticsearch_dsl import Search, Q, A
from tests.helper import TestHelpers
from api.elastic import ElasticClient, ElasticClientError


class TestElasticClient:
    @classmethod
    def setup_class(cls):
        TestHelpers.setEnvVars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clearEnvVars()

    @pytest.fixture
    def testInstance(self):
        class MockElasticClient(ElasticClient):
            def __init__(self):
                self.esIndex = "test_es_index"
                self.environment = "test"
                self.searchedFields = []

        return MockElasticClient()

    @pytest.fixture
    def mockSearch(self, mocker):
        mockSearch = mocker.MagicMock(name="mockSearch")
        mockResults = mocker.MagicMock(name="mockRes")
        mockHit = mocker.MagicMock(meta=mocker.MagicMock(sort=["testSort"]))
        mockResults.hits = [mockHit]
        mockSearch.query.return_value = mockSearch
        mockSearch.extra.return_value = mockSearch
        mockSearch.__getitem__.return_value = mockSearch
        mockSearch.execute.return_value = mockResults

        return mockSearch

    @pytest.fixture
    def searchMocks(self, mocker):
        return mocker.patch.multiple(
            ElasticClient,
            getFromSize=mocker.DEFAULT,
            createSearch=mocker.DEFAULT,
            titleQuery=mocker.DEFAULT,
            authorQuery=mocker.DEFAULT,
            subjectQuery=mocker.DEFAULT,
            authorityQuery=mocker.DEFAULT,
            identifierQuery=mocker.DEFAULT,
            createFilterClausesAndAggregations=mocker.DEFAULT,
            addSortClause=mocker.DEFAULT,
            addFiltersAndAggregations=mocker.DEFAULT,
            addSearchHighlighting=mocker.DEFAULT,
            escapeSearchQuery=mocker.DEFAULT,
            generateQueryHash=mocker.DEFAULT,
            getPageResultCache=mocker.DEFAULT,
            setPageResultCache=mocker.DEFAULT,
        )

    def test_createSearch(self, testInstance, mocker):
        mockSearch = mocker.patch("api.elastic.Search")
        mockSearchClient = mocker.MagicMock(track_total_hits="True")
        mockSearch.return_value = mockSearchClient

        searchClient = testInstance.createSearch()

        assert searchClient == mockSearchClient.params()
        mockSearch.assert_called_once_with(index="test_es_index")

    def test_searchQuery(self, testInstance, mocker):
        mockGenerate = mocker.patch.object(ElasticClient, "generateSearchQuery")
        mockExecute = mocker.patch.object(ElasticClient, "executeSearchQuery")
        mockExecute.return_value = "testResponse"

        assert testInstance.searchQuery("testParams") == "testResponse"

        mockGenerate.assert_called_once_with("testParams")
        mockExecute.assert_called_once_with("testParams", 0, 10)

    def test_generateSearchQuery_keyword_search(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["createSearch"].return_value = mockSearch
        searchMocks["escapeSearchQuery"].return_value = "escapedQuery"
        searchMocks["titleQuery"].return_value = "titleQuery"
        searchMocks["authorQuery"].return_value = "authorQuery"
        searchMocks["subjectQuery"].return_value = "subjectQuery"
        searchMocks["createFilterClausesAndAggregations"].return_value = mockSearch
        searchMocks["addSortClause"].return_value = mockSearch

        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["testQuery1", "testQuery2", "searchClauses"]

        testInstance.generateSearchQuery(
            {
                "query": [("keyword", "test"), (None, "test2")],
                "sort": ["sort"],
                "filter": ["filter"],
            }
        )

        mockQuery.assert_has_calls(
            [
                mocker.call(
                    "bool", should=["titleQuery", "authorQuery", "subjectQuery"]
                ),
                mocker.call(
                    "bool", should=["titleQuery", "authorQuery", "subjectQuery"]
                ),
                mocker.call("bool", must=["testQuery1", "testQuery2"]),
            ]
        )
        searchMocks["escapeSearchQuery"].assert_has_calls(
            [mocker.call("test"), mocker.call("test2")]
        )
        searchMocks["titleQuery"].assert_has_calls(
            [mocker.call("escapedQuery"), mocker.call("escapedQuery")]
        )
        searchMocks["authorQuery"].assert_has_calls(
            [mocker.call("escapedQuery"), mocker.call("escapedQuery")]
        )
        searchMocks["subjectQuery"].assert_has_calls(
            [mocker.call("escapedQuery"), mocker.call("escapedQuery")]
        )
        searchMocks["authorityQuery"].assert_not_called()

        mockSearch.query.assert_called_once_with("searchClauses")

        searchMocks["createFilterClausesAndAggregations"].assert_called_once_with(
            ["filter"]
        )
        searchMocks["addSortClause"].assert_called_once_with(["sort"])
        searchMocks["addFiltersAndAggregations"].assert_called_once_with(3)
        searchMocks["addSearchHighlighting"].assert_called_once()

    def test_generateSearchQuery_title_search(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["createSearch"].return_value = mockSearch
        searchMocks["escapeSearchQuery"].return_value = "escapedQuery"
        searchMocks["titleQuery"].return_value = "titleQuery"
        searchMocks["createFilterClausesAndAggregations"].return_value = mockSearch
        searchMocks["addSortClause"].return_value = mockSearch

        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["searchClauses"]

        testInstance.generateSearchQuery(
            {
                "query": [
                    ("title", "test"),
                ],
                "sort": ["sort"],
                "filter": ["filter"],
            }
        )

        mockQuery.assert_called_once_with("bool", must=["titleQuery"])
        searchMocks["escapeSearchQuery"].assert_called_once_with("test")
        searchMocks["titleQuery"].assert_called_once_with("escapedQuery")
        searchMocks["authorQuery"].assert_not_called()
        searchMocks["subjectQuery"].assert_not_called()
        searchMocks["authorityQuery"].assert_not_called()

        mockSearch.query.assert_called_once_with("searchClauses")

        searchMocks["createFilterClausesAndAggregations"].assert_called_once_with(
            ["filter"]
        )
        searchMocks["addSortClause"].assert_called_once_with(["sort"])
        searchMocks["addFiltersAndAggregations"].assert_called_once_with(3)
        searchMocks["addSearchHighlighting"].assert_called_once()

    def test_generateSearchQuery_author_search(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["createSearch"].return_value = mockSearch
        searchMocks["escapeSearchQuery"].return_value = "escapedQuery"
        searchMocks["authorQuery"].return_value = "authorQuery"
        searchMocks["createFilterClausesAndAggregations"].return_value = mockSearch
        searchMocks["addSortClause"].return_value = mockSearch

        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["searchClauses"]

        testInstance.generateSearchQuery(
            {
                "query": [
                    ("author", "test"),
                ],
                "sort": ["sort"],
                "filter": ["filter"],
            }
        )

        mockQuery.assert_called_once_with("bool", must=["authorQuery"])
        searchMocks["escapeSearchQuery"].assert_called_once_with("test")
        searchMocks["titleQuery"].assert_not_called()
        searchMocks["authorQuery"].assert_called_once_with("escapedQuery")
        searchMocks["subjectQuery"].assert_not_called()
        searchMocks["authorityQuery"].assert_not_called()

        mockSearch.query.assert_called_once_with("searchClauses")

        searchMocks["createFilterClausesAndAggregations"].assert_called_once_with(
            ["filter"]
        )
        searchMocks["addSortClause"].assert_called_once_with(["sort"])
        searchMocks["addFiltersAndAggregations"].assert_called_once_with(3)
        searchMocks["addSearchHighlighting"].assert_called_once()

    def test_generateSearchQuery_subject_search(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["createSearch"].return_value = mockSearch
        searchMocks["escapeSearchQuery"].return_value = "escapedQuery"
        searchMocks["subjectQuery"].return_value = "subjectQuery"
        searchMocks["createFilterClausesAndAggregations"].return_value = mockSearch
        searchMocks["addSortClause"].return_value = mockSearch

        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["searchClauses"]

        testInstance.generateSearchQuery(
            {
                "query": [
                    ("subject", "test"),
                ],
                "sort": ["sort"],
                "filter": ["filter"],
            }
        )

        mockQuery.assert_called_once_with("bool", must=["subjectQuery"])
        searchMocks["escapeSearchQuery"].assert_called_once_with("test")
        searchMocks["titleQuery"].assert_not_called()
        searchMocks["authorQuery"].assert_not_called()
        searchMocks["subjectQuery"].assert_called_once_with("escapedQuery")
        searchMocks["authorityQuery"].assert_not_called()

        mockSearch.query.assert_called_once_with("searchClauses")

        searchMocks["createFilterClausesAndAggregations"].assert_called_once_with(
            ["filter"]
        )
        searchMocks["addSortClause"].assert_called_once_with(["sort"])
        searchMocks["addFiltersAndAggregations"].assert_called_once_with(3)
        searchMocks["addSearchHighlighting"].assert_called_once()

    def test_generateSearchQuery_authority_search(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["createSearch"].return_value = mockSearch
        searchMocks["escapeSearchQuery"].return_value = "escapedQuery"
        searchMocks["authorityQuery"].return_value = "authorityQuery"
        searchMocks["createFilterClausesAndAggregations"].return_value = mockSearch
        searchMocks["addSortClause"].return_value = mockSearch

        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["searchClauses"]

        testInstance.generateSearchQuery(
            {
                "query": [
                    ("viaf", "test"),
                ],
                "sort": ["sort"],
                "filter": ["filter"],
            }
        )

        mockQuery.assert_called_once_with("bool", must=["authorityQuery"])
        searchMocks["escapeSearchQuery"].assert_called_once_with("test")
        searchMocks["titleQuery"].assert_not_called()
        searchMocks["authorQuery"].assert_not_called()
        searchMocks["subjectQuery"].assert_not_called()
        searchMocks["authorityQuery"].assert_called_once_with("viaf", "test")
        searchMocks["identifierQuery"].assert_not_called()

        mockSearch.query.assert_called_once_with("searchClauses")

        searchMocks["createFilterClausesAndAggregations"].assert_called_once_with(
            ["filter"]
        )
        searchMocks["addSortClause"].assert_called_once_with(["sort"])
        searchMocks["addFiltersAndAggregations"].assert_called_once_with(3)
        searchMocks["addSearchHighlighting"].assert_called_once()

    def test_generateSearchQuery_identifier_search(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["createSearch"].return_value = mockSearch
        searchMocks["escapeSearchQuery"].return_value = "escapedQuery"
        searchMocks["identifierQuery"].return_value = "identifierQuery"
        searchMocks["createFilterClausesAndAggregations"].return_value = mockSearch
        searchMocks["addSortClause"].return_value = mockSearch

        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["searchClauses"]

        testInstance.generateSearchQuery(
            {
                "query": [
                    ("identifier", "test"),
                ],
                "sort": ["sort"],
                "filter": ["filter"],
            }
        )

        mockQuery.assert_called_once_with("bool", must=["identifierQuery"])
        searchMocks["escapeSearchQuery"].assert_called_once_with("test")
        searchMocks["titleQuery"].assert_not_called()
        searchMocks["authorQuery"].assert_not_called()
        searchMocks["subjectQuery"].assert_not_called()
        searchMocks["authorityQuery"].assert_not_called()
        searchMocks["identifierQuery"].assert_called_once_with(
            [
                "isbn",
                "issn",
                "lcc",
                "lccn",
                "oclc",
                "owi",
                "nypl",
                "hathi",
                "gutenberg",
                "doab",
                "doi",
            ],
            "test",
        )

        mockSearch.query.assert_called_once_with("searchClauses")

        searchMocks["createFilterClausesAndAggregations"].assert_called_once_with(
            ["filter"]
        )
        searchMocks["addSortClause"].assert_called_once_with(["sort"])
        searchMocks["addFiltersAndAggregations"].assert_called_once_with(3)
        searchMocks["addSearchHighlighting"].assert_called_once()

    def test_generateSearchQuery_generic_search(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["createSearch"].return_value = mockSearch
        searchMocks["escapeSearchQuery"].return_value = "escapedQuery"
        searchMocks["createFilterClausesAndAggregations"].return_value = mockSearch
        searchMocks["addSortClause"].return_value = mockSearch

        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["genericQuery", "searchClauses"]

        testInstance.generateSearchQuery(
            {
                "query": [
                    ("other", "test"),
                ],
                "sort": ["sort"],
                "filter": ["filter"],
            }
        )

        searchMocks["escapeSearchQuery"].assert_called_once_with("test")
        mockQuery.assert_has_calls(
            [
                mocker.call("match", other="escapedQuery"),
                mocker.call("bool", must=["genericQuery"]),
            ]
        )
        searchMocks["titleQuery"].assert_not_called()
        searchMocks["authorQuery"].assert_not_called()
        searchMocks["subjectQuery"].assert_not_called()
        searchMocks["authorityQuery"].assert_not_called()

        mockSearch.query.assert_called_once_with("searchClauses")

        searchMocks["createFilterClausesAndAggregations"].assert_called_once_with(
            ["filter"]
        )
        searchMocks["addSortClause"].assert_called_once_with(["sort"])
        searchMocks["addFiltersAndAggregations"].assert_called_once_with(3)
        searchMocks["addSearchHighlighting"].assert_called_once()

    def test_generateSearchQuery_multi_search(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["createSearch"].return_value = mockSearch
        searchMocks["escapeSearchQuery"].return_value = "escapedQuery"
        searchMocks["authorityQuery"].return_value = "authorityQuery"
        searchMocks["titleQuery"].return_value = "titleQuery"
        searchMocks["createFilterClausesAndAggregations"].return_value = mockSearch
        searchMocks["addSortClause"].return_value = mockSearch

        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["searchClauses"]

        testInstance.generateSearchQuery(
            {
                "query": [("title", "testTitle"), ("lcnaf", "test")],
                "sort": ["sort"],
                "filter": ["filter"],
            }
        )

        searchMocks["escapeSearchQuery"].assert_has_calls(
            [mocker.call("testTitle"), mocker.call("test")]
        )
        mockQuery.assert_called_once_with("bool", must=["titleQuery", "authorityQuery"])
        searchMocks["titleQuery"].assert_called_once_with("escapedQuery")
        searchMocks["authorQuery"].assert_not_called()
        searchMocks["subjectQuery"].assert_not_called()
        searchMocks["authorityQuery"].assert_called_once_with("lcnaf", "test")
        searchMocks["identifierQuery"].assert_not_called()

        mockSearch.query.assert_called_once_with("searchClauses")

        searchMocks["createFilterClausesAndAggregations"].assert_called_once_with(
            ["filter"]
        )
        searchMocks["addSortClause"].assert_called_once_with(["sort"])
        searchMocks["addFiltersAndAggregations"].assert_called_once_with(3)
        searchMocks["addSearchHighlighting"].assert_called_once()

    def test_executeSearchQuery_standard(self, testInstance, mockSearch, searchMocks):
        searchMocks["getFromSize"].return_value = (0, 10)
        searchMocks["generateQueryHash"].return_value = "testHash"
        searchMocks["getPageResultCache"].return_value = None

        testInstance.query = mockSearch

        testResult = testInstance.executeSearchQuery({}, 0, 10)

        assert testResult._extract_mock_name() == "mockRes"

        searchMocks["getFromSize"].assert_called_once_with(0, 10)
        searchMocks["generateQueryHash"].assert_called_once_with({}, 0)
        searchMocks["getPageResultCache"].assert_not_called()
        searchMocks["setPageResultCache"].assert_called_once_with(
            "testHash", ["testSort"]
        )

    def test_executeSearchQuery_no_results(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["getFromSize"].return_value = (0, 10)
        searchMocks["generateQueryHash"].return_value = "testHash"
        searchMocks["getPageResultCache"].return_value = None

        mockEmptyRes = mocker.MagicMock(name="mockRes", hits=[])
        mockSearch.execute.return_value = mockEmptyRes

        testInstance.query = mockSearch

        testResult = testInstance.executeSearchQuery({}, 0, 10)

        assert testResult._extract_mock_name() == "mockRes"

        searchMocks["getFromSize"].assert_called_once_with(0, 10)
        searchMocks["generateQueryHash"].assert_called_once_with({}, 0)
        searchMocks["getPageResultCache"].assert_not_called()
        searchMocks["setPageResultCache"].assert_not_called()

    def test_executeSearchQuery_cached(self, testInstance, mockSearch, searchMocks):
        searchMocks["getFromSize"].return_value = (10, 20)
        searchMocks["generateQueryHash"].return_value = "testHash"
        searchMocks["getPageResultCache"].return_value = b"test|sort"

        testInstance.query = mockSearch

        testResult = testInstance.executeSearchQuery({}, 10, 10)

        assert testResult._extract_mock_name() == "mockRes"

        mockSearch.extra.assert_called_once_with(search_after=["test", "sort"])
        searchMocks["getFromSize"].assert_called_once_with(10, 10)
        searchMocks["generateQueryHash"].assert_called_once_with({}, 10)
        searchMocks["getPageResultCache"].assert_called_once_with("testHash")
        searchMocks["setPageResultCache"].assert_not_called()

    def test_executeSearchQuery_reverse(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["getFromSize"].return_value = (7000, 7010)
        searchMocks["generateQueryHash"].return_value = "testHash"
        searchMocks["getPageResultCache"].return_value = None
        mockExecuteReversed = mocker.patch.object(ElasticClient, "executeReversedQuery")
        mockExecuteReversed.return_value = mockSearch.execute()

        mockSearch.count.return_value = 7500
        testInstance.query = mockSearch

        testResult = testInstance.executeSearchQuery({}, 700, 10)

        assert testResult._extract_mock_name() == "mockRes"

        searchMocks["getFromSize"].assert_called_once_with(700, 10)
        searchMocks["generateQueryHash"].assert_called_once_with({}, 7000)
        searchMocks["getPageResultCache"].assert_called_once_with("testHash")
        mockExecuteReversed.assert_called_once_with({}, 7500, 7000, 10)
        searchMocks["setPageResultCache"].assert_called_once_with(
            "testHash", ["testSort"]
        )

    def test_executeSearchQuery_deep(
        self, testInstance, mockSearch, searchMocks, mocker
    ):
        searchMocks["getFromSize"].return_value = (7000, 7010)
        searchMocks["generateQueryHash"].return_value = "testHash"
        searchMocks["getPageResultCache"].return_value = None
        mockDeepReversed = mocker.patch.object(ElasticClient, "executeDeepQuery")
        mockDeepReversed.return_value = mockSearch.execute()

        mockSearch.count.return_value = 17500
        testInstance.query = mockSearch

        testResult = testInstance.executeSearchQuery({}, 700, 10)

        assert testResult._extract_mock_name() == "mockRes"

        searchMocks["getFromSize"].assert_called_once_with(700, 10)
        searchMocks["generateQueryHash"].assert_called_once_with({}, 7000)
        searchMocks["getPageResultCache"].assert_called_once_with("testHash")
        mockDeepReversed.assert_called_once_with(7000, 10)
        searchMocks["setPageResultCache"].assert_called_once_with(
            "testHash", ["testSort"]
        )

    def test_executeReversedQuery(self, testInstance, mockSearch, searchMocks):
        mockSearch.sort.return_value = mockSearch
        testInstance.query = mockSearch

        testResult = testInstance.executeReversedQuery(
            {"sort": "test"}, 10000, 9000, 10
        )

        assert testResult._extract_mock_name() == "mockRes"
        assert testInstance.sortReversed == True

        searchMocks["addSortClause"].assert_called_once_with("test", reverse=True)

    def test_executeDeepQuery(self, testInstance, mockSearch, mocker):
        mockSearch.extra.return_value = mockSearch
        mockSearch.source.return_value = mockSearch
        testInstance.query = mockSearch

        testResult = testInstance.executeDeepQuery(7500, 10)

        assert testResult._extract_mock_name() == "mockRes"
        mockSearch.extra.assert_has_calls(
            [
                mocker.call(search_after=["testSort"]),
                mocker.call(search_after=["testSort"]),
            ]
        )
        mockSearch.source.assert_has_calls([mocker.call(False), mocker.call(True)])

    def test_setPageResultCache(self, testInstance, mocker):
        testInstance.redis = mocker.MagicMock()

        testInstance.setPageResultCache("testCacheKey", ["Test", "Sort"])

        testInstance.redis.set.assert_called_once_with(
            "test/queryPaging/testCacheKey", "Test|Sort", ex=86400
        )

    def test_getPageResultCache(self, testInstance, mocker):
        testInstance.redis = mocker.MagicMock()

        testInstance.getPageResultCache("testCacheKey")

        testInstance.redis.get("test/queryPaging/testCacheKey")

    def test_generateQueryHash(self, mocker):
        mockMakeHashable = mocker.patch.object(ElasticClient, "makeDictHashable")
        mockMakeHashable.return_value = "testHashDict"

        assert (
            ElasticClient.generateQueryHash({}, 1)
            == "1711934bfb75c5942d7683190e93b7efc5d89274"
        )

    def test_makeDictHashable(self, mocker):
        testHashable = ElasticClient.makeDictHashable(
            {
                "test": "string",
                "test2": ["an", "array", {"of": "objects"}],
                "alttest": {"nested": "object"},
            }
        )

        testHashable == (
            ("alttest", (("nested", "object"),)),
            ("test", "string"),
            ("test2", ("an", "array", (("of", "objects"),))),
        )

    def test_escapeSearchQuery_changed(self):
        assert (
            ElasticClient.escapeSearchQuery("/[test]+a:thing!")
            == "\/\[test\]\+a\:thing\!"
        )

    def test_escapeSearchQuery_unchanged(self):
        assert ElasticClient.escapeSearchQuery("a simple query") == "a simple query"

    def test_titleQuery(self, testInstance):
        testQueryES = testInstance.titleQuery("testTitle")
        testQuery = testQueryES.to_dict()

        assert testInstance.searchedFields == ["title", "alt_titles", "editions.title"]
        assert testQuery["bool"]["should"][0]["query_string"]["query"] == "testTitle"
        assert testQuery["bool"]["should"][0]["query_string"]["fields"] == [
            "title.*^3",
            "alt_titles.*",
        ]
        assert testQuery["bool"]["should"][1]["nested"]["path"] == "editions"
        assert (
            testQuery["bool"]["should"][1]["nested"]["query"]["query_string"]["query"]
            == "testTitle"
        )
        assert testQuery["bool"]["should"][1]["nested"]["query"]["query_string"][
            "fields"
        ] == ["editions.title.*"]
        assert (
            testQuery["bool"]["should"][1]["nested"]["query"]["query_string"][
                "default_operator"
            ]
            == "and"
        )

    def test_authorQuery(self, testInstance):
        testQueryES = testInstance.authorQuery("testAuthor")
        testQuery = testQueryES.to_dict()

        assert testInstance.searchedFields == ["agents.name", "editions.agents.name"]
        assert testQuery["bool"]["should"][0]["nested"]["path"] == "agents"
        assert (
            testQuery["bool"]["should"][0]["nested"]["query"]["bool"]["must"][0][
                "query_string"
            ]["query"]
            == "testAuthor"
        )
        assert testQuery["bool"]["should"][0]["nested"]["query"]["bool"]["must"][0][
            "query_string"
        ]["fields"] == ["agents.name^2"]
        assert (
            testQuery["bool"]["should"][0]["nested"]["query"]["bool"]["must"][1][
                "terms"
            ]["agents.roles"]
            == ElasticClient.ROLE_ALLOWLIST
        )
        assert testQuery["bool"]["should"][1]["nested"]["path"] == "editions.agents"
        assert (
            testQuery["bool"]["should"][1]["nested"]["query"]["bool"]["must"][0][
                "query_string"
            ]["query"]
            == "testAuthor"
        )
        assert testQuery["bool"]["should"][1]["nested"]["query"]["bool"]["must"][0][
            "query_string"
        ]["fields"] == ["editions.agents.name"]
        assert (
            testQuery["bool"]["should"][1]["nested"]["query"]["bool"]["must"][1][
                "terms"
            ]["editions.agents.roles"]
            == ElasticClient.ROLE_ALLOWLIST
        )

    def test_authorityQuery(self, testInstance):
        testQueryES = testInstance.authorityQuery("testAuth", "testID")
        testQuery = testQueryES.to_dict()

        assert testInstance.searchedFields == [
            "agents.testAuth",
            "editions.agents.testAuth",
        ]
        assert testQuery["bool"]["should"][0]["nested"]["path"] == "agents"
        assert (
            testQuery["bool"]["should"][0]["nested"]["query"]["term"]["agents.testAuth"]
            == "testID"
        )
        assert testQuery["bool"]["should"][1]["nested"]["path"] == "editions.agents"
        assert (
            testQuery["bool"]["should"][1]["nested"]["query"]["term"][
                "editions.agents.testAuth"
            ]
            == "testID"
        )

    def test_subjectQuery(self, testInstance):
        testQueryES = testInstance.subjectQuery("testSubject")
        testQuery = testQueryES.to_dict()

        assert testInstance.searchedFields == ["subjects.heading"]
        assert testQuery["nested"]["path"] == "subjects"
        assert testQuery["nested"]["query"]["query_string"]["query"] == "testSubject"
        assert testQuery["nested"]["query"]["query_string"]["fields"] == [
            "subjects.heading.*"
        ]

    def test_identifierQuery_OnlyIdentifier(self, testInstance):
        testQueryES = testInstance.identifierQuery(["testIdent"], "testIdent")
        testQuery = testQueryES.to_dict()

        assert testInstance.searchedFields == [
            "identifiers.identifier",
            "editions.identifiers.identifier",
            "identifiers.authority",
            "editions.identifiers.authority",
        ]
        assert testQuery["bool"]["should"][0]["nested"]["path"] == "identifiers"
        assert testQuery["bool"]["should"][0]["nested"]["query"]["bool"]["must"][0][
            "term"
        ] == {"identifiers.identifier": "testIdent"}
        assert (
            testQuery["bool"]["should"][1]["nested"]["path"] == "editions.identifiers"
        )
        assert testQuery["bool"]["should"][1]["nested"]["query"]["bool"]["must"][0][
            "term"
        ] == {"editions.identifiers.identifier": "testIdent"}

    def test_identifierQuery_AuthIdent(self, testInstance):
        testQueryES = testInstance.identifierQuery(["testAuth"], "testAuth|testIdent")
        testQuery = testQueryES.to_dict()

        assert testInstance.searchedFields == [
            "identifiers.identifier",
            "editions.identifiers.identifier",
            "identifiers.authority",
            "editions.identifiers.authority",
        ]
        assert testQuery["bool"]["should"][0]["nested"]["path"] == "identifiers"
        assert testQuery["bool"]["should"][0]["nested"]["query"]["bool"]["must"][0][
            "term"
        ] == {"identifiers.authority": "testAuth"}
        assert testQuery["bool"]["should"][0]["nested"]["query"]["bool"]["must"][1][
            "term"
        ] == {"identifiers.identifier": "testIdent"}
        assert (
            testQuery["bool"]["should"][1]["nested"]["path"] == "editions.identifiers"
        )
        assert testQuery["bool"]["should"][1]["nested"]["query"]["bool"]["must"][0][
            "term"
        ] == {"editions.identifiers.authority": "testAuth"}
        assert testQuery["bool"]["should"][1]["nested"]["query"]["bool"]["must"][1][
            "term"
        ] == {"editions.identifiers.identifier": "testIdent"}

    def test_getFromSize(self):
        startPosition, endPosition = ElasticClient.getFromSize(3, 15)

        assert startPosition == 45
        assert endPosition == 60

    def test_getFromSize_initial(self):
        startPosition, endPosition = ElasticClient.getFromSize(0, 20)
        assert startPosition == 0
        assert endPosition == 20

    def test_addSortClause_title_w_direction(self, testInstance, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.sort.return_value = "sortQuery"

        testInstance.query = mockQuery
        testInstance.addSortClause([("title", "DESC")])

        assert testInstance.query == "sortQuery"
        mockQuery.sort.assert_called_once_with(
            {"sort_title": {"order": "desc"}}, {"uuid": "asc"}
        )

    def test_addSortClause_title_wo_direction(self, testInstance, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.sort.return_value = "sortQuery"

        testInstance.query = mockQuery
        testInstance.addSortClause([("title", None)])

        assert testInstance.query == "sortQuery"
        mockQuery.sort.assert_called_once_with(
            {"sort_title": {"order": "asc"}}, {"uuid": "asc"}
        )

    def test_addSortClause_author(self, testInstance, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.sort.return_value = "sortQuery"

        testInstance.query = mockQuery
        testInstance.addSortClause([("author", "ASC")])

        assert testInstance.query == "sortQuery"
        mockQuery.sort.assert_called_once_with(
            {
                "agents.sort_name": {
                    "order": "asc",
                    "nested": {
                        "path": "agents",
                        "filter": {"terms": {"agents.roles": ["author"]}},
                        "max_children": 1,
                    },
                }
            },
            {"uuid": "asc"},
        )

    def test_addSortClause_date_w_filters(self, testInstance, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.sort.return_value = "sortQuery"

        mockFilter = mocker.MagicMock()
        mockFilter.to_dict.return_value = "testFilter"

        testInstance.appliedFilters = [mockFilter]
        testInstance.languageFilters = []

        testInstance.query = mockQuery
        testInstance.addSortClause([("date", "DESC")])

        assert testInstance.query == "sortQuery"
        mockQuery.sort.assert_called_once_with(
            {
                "editions.publication_date": {
                    "order": "desc",
                    "nested": {
                        "path": "editions",
                        "filter": {"bool": {"must": ["testFilter"]}},
                    },
                }
            },
            {"uuid": "asc"},
        )

    def test_addSortClause_date_w_filters_and_language(self, testInstance, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.sort.return_value = "sortQuery"

        mockFilter = mocker.MagicMock()
        mockFilter.to_dict.side_effect = ["testFilter", "testLangFilter"]

        testInstance.appliedFilters = [mockFilter]
        testInstance.languageFilters = [mockFilter]

        testInstance.query = mockQuery
        testInstance.addSortClause([("date", "DESC")])

        assert testInstance.query == "sortQuery"
        mockQuery.sort.assert_called_once_with(
            {
                "editions.publication_date": {
                    "order": "desc",
                    "nested": {
                        "path": "editions",
                        "filter": {"bool": {"must": ["testFilter", "testLangFilter"]}},
                    },
                }
            },
            {"uuid": "asc"},
        )

    def test_addSortClause_root_sort_only(self, testInstance, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.sort.return_value = "sortQuery"

        testInstance.query = mockQuery
        testInstance.addSortClause([])

        assert testInstance.query == "sortQuery"
        mockQuery.sort.assert_called_once_with({"_score": "desc"}, {"uuid": "asc"})

    def test_addSortClause_reverse_true(self, testInstance, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.sort.return_value = "sortQuery"

        testInstance.query = mockQuery
        testInstance.addSortClause([("title", "DESC")], reverse=True)

        assert testInstance.query == "sortQuery"
        mockQuery.sort.assert_called_once_with(
            {"sort_title": {"order": "asc"}}, {"uuid": "desc"}
        )

    def test_createFilterClausesAndAggregations_default(self, testInstance, mocker):
        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.return_value = "formatFilter"
        mockAgg = mocker.patch("api.elastic.A")
        mockAgg.return_value = "formatAggregation"

        testInstance.createFilterClausesAndAggregations([])

        mockQuery.assert_called_once_with("exists", field="editions.formats")
        mockAgg.assert_called_once_with("filter", exists={"field": "editions.formats"})

        assert testInstance.appliedFilters == ["formatFilter"]
        assert testInstance.appliedAggregations == ["formatAggregation"]

    def test_createFilterClausesAndAggregations_w_date(self, testInstance, mocker):
        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["dateFilter", "formatFilter"]
        mockAgg = mocker.patch("api.elastic.A")
        mockAgg.side_effect = ["dateAggregation", "formatAggregation"]

        mockGenerateRange = mocker.patch.object(ElasticClient, "generateDateRange")
        mockGenerateRange.return_value = "testRange"

        testInstance.createFilterClausesAndAggregations([("startYear", 1900)])

        mockQuery.assert_has_calls(
            [
                mocker.call("exists", field="editions.formats"),
                mocker.call("range", **{"editions.publication_date": "testRange"}),
            ]
        )
        mockAgg.assert_has_calls(
            [
                mocker.call("filter", exists={"field": "editions.formats"}),
                mocker.call("filter", range={"editions.publication_date": "testRange"}),
            ]
        )

        assert testInstance.appliedFilters == ["formatFilter", "dateFilter"]
        assert testInstance.appliedAggregations == [
            "formatAggregation",
            "dateAggregation",
        ]

    def test_createFilterClausesAndAggregations_w_format(self, testInstance, mocker):
        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["displayFilter", "formatFilter"]
        mockAgg = mocker.patch("api.elastic.A")
        mockAgg.side_effect = ["displayAggregation", "formatAggregation"]

        testInstance.createFilterClausesAndAggregations(
            [("format", "pdf"), ("format", "html_edd")]
        )

        mockQuery.assert_has_calls(
            [
                mocker.call("exists", field="editions.formats"),
                mocker.call(
                    "terms",
                    editions__formats=[
                        "application/pdf",
                        "application/html+edd",
                        "application/x.html+edd",
                    ],
                ),
            ]
        )
        mockAgg.assert_has_calls(
            [
                mocker.call("filter", exists={"field": "editions.formats"}),
                mocker.call(
                    "filter",
                    terms={
                        "editions.formats": [
                            "application/pdf",
                            "application/html+edd",
                            "application/x.html+edd",
                        ]
                    },
                ),
            ]
        )

        assert testInstance.appliedFilters == ["formatFilter", "displayFilter"]
        assert testInstance.appliedAggregations == [
            "formatAggregation",
            "displayAggregation",
        ]

    def test_createFilterClausesAndAggregations_w_format_readable(
        self, testInstance, mocker
    ):
        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["displayFilter", "formatFilter"]
        mockAgg = mocker.patch("api.elastic.A")
        mockAgg.side_effect = ["displayAggregation", "formatAggregation"]

        testInstance.createFilterClausesAndAggregations([("format", "readable")])

        mockQuery.assert_has_calls(
            [
                mocker.call("exists", field="editions.formats"),
                mocker.call(
                    "terms",
                    editions__formats=[
                        "application/epub+xml",
                        "text/html",
                        "application/webpub+json",
                    ],
                ),
            ]
        )
        mockAgg.assert_has_calls(
            [
                mocker.call("filter", exists={"field": "editions.formats"}),
                mocker.call(
                    "filter",
                    terms={
                        "editions.formats": [
                            "application/epub+xml",
                            "text/html",
                            "application/webpub+json",
                        ]
                    },
                ),
            ]
        )

        assert testInstance.appliedFilters == ["formatFilter", "displayFilter"]
        assert testInstance.appliedAggregations == [
            "formatAggregation",
            "displayAggregation",
        ]

    def test_createFilterClausesAndAggregations_w_format_downloadable(
        self, testInstance, mocker
    ):
        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["displayFilter", "formatFilter"]
        mockAgg = mocker.patch("api.elastic.A")
        mockAgg.side_effect = ["displayAggregation", "formatAggregation"]

        testInstance.createFilterClausesAndAggregations([("format", "downloadable")])

        mockQuery.assert_has_calls(
            [
                mocker.call("exists", field="editions.formats"),
                mocker.call(
                    "terms",
                    editions__formats=["application/pdf", "application/epub+zip"],
                ),
            ]
        )
        mockAgg.assert_has_calls(
            [
                mocker.call("filter", exists={"field": "editions.formats"}),
                mocker.call(
                    "filter",
                    terms={
                        "editions.formats": [
                            "application/pdf",
                            "application/epub+zip",
                        ]
                    },
                ),
            ]
        )

        assert testInstance.appliedFilters == ["formatFilter", "displayFilter"]
        assert testInstance.appliedAggregations == [
            "formatAggregation",
            "displayAggregation",
        ]

    def test_createFilterClausesAndAggregations_w_format_requestable(
        self, testInstance, mocker
    ):
        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["displayFilter", "formatFilter"]
        mockAgg = mocker.patch("api.elastic.A")
        mockAgg.side_effect = ["displayAggregation", "formatAggregation"]

        testInstance.createFilterClausesAndAggregations([("format", "requestable")])

        mockQuery.assert_has_calls(
            [
                mocker.call("exists", field="editions.formats"),
                mocker.call(
                    "terms",
                    editions__formats=[
                        "application/html+edd",
                        "application/x.html+edd",
                    ],
                ),
            ]
        )

        mockAgg.assert_has_calls(
            [
                mocker.call("filter", exists={"field": "editions.formats"}),
                mocker.call(
                    "filter",
                    terms={
                        "editions.formats": [
                            "application/html+edd",
                            "application/x.html+edd",
                        ]
                    },
                ),
            ]
        )

        assert testInstance.appliedFilters == ["formatFilter", "displayFilter"]
        assert testInstance.appliedAggregations == [
            "formatAggregation",
            "displayAggregation",
        ]

    def test_createFilterClausesAndAggregations_w_format_error(self, testInstance):
        with pytest.raises(ElasticClientError):
            testInstance.createFilterClausesAndAggregations([("format", "test")])

    def test_createFilterClausesAndAggregations_w_language(self, testInstance, mocker):
        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["displayFilter", "innerLang", "languageFilter"]
        mockAgg = mocker.patch("api.elastic.A")
        mockAgg.side_effect = ["displayAggregation"]

        testInstance.createFilterClausesAndAggregations([("language", "Test1")])

        mockQuery.assert_has_calls(
            [
                mocker.call("exists", field="editions.formats"),
                mocker.call("term", editions__languages__language="Test1"),
                mocker.call("nested", path="editions.languages", query="innerLang"),
            ]
        )
        mockAgg.assert_has_calls(
            [
                mocker.call("filter", exists={"field": "editions.formats"}),
            ]
        )

        assert testInstance.languageFilters == ["languageFilter"]
        assert testInstance.appliedFilters == ["displayFilter"]
        assert testInstance.appliedAggregations == ["displayAggregation"]

    def test_createFilterClausesAndAggregations_w_noGovDoc(self, testInstance, mocker):
        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["displayFilter", "govDocFilters"]
        mockAgg = mocker.patch("api.elastic.A")
        mockAgg.side_effect = ["displayAggregation", "govDocAggregation"]

        testInstance.createFilterClausesAndAggregations([("govDoc", "noGovDoc")])

        mockQuery.assert_has_calls(
            [
                mocker.call("exists", field="editions.formats"),
                mocker.call("term", **{"is_government_document": False}),
            ]
        )
        mockAgg.assert_has_calls(
            [
                mocker.call("filter", exists={"field": "editions.formats"}),
                mocker.call("filter", **{"term": {"is_government_document": False}}),
            ]
        )

        assert testInstance.govDocFilter == "govDocFilters"
        assert testInstance.appliedFilters == ["displayFilter"]
        assert testInstance.appliedAggregations == ["displayAggregation"]

    def test_createFilterClausesAndAggregations_w_onlyGovDoc(
        self, testInstance, mocker
    ):
        mockQuery = mocker.patch("api.elastic.Q")
        mockQuery.side_effect = ["displayFilter", "govDocFilters"]
        mockAgg = mocker.patch("api.elastic.A")
        mockAgg.side_effect = ["displayAggregation", "govDocAggregation"]

        testInstance.createFilterClausesAndAggregations([("govDoc", "onlyGovDoc")])

        mockQuery.assert_has_calls(
            [
                mocker.call("exists", field="editions.formats"),
                mocker.call("term", **{"is_government_document": True}),
            ]
        )
        mockAgg.assert_has_calls(
            [mocker.call("filter", exists={"field": "editions.formats"})]
        )

        assert testInstance.govDocFilter == "govDocFilters"
        assert testInstance.appliedFilters == ["displayFilter"]
        assert testInstance.appliedAggregations == ["displayAggregation"]

    def test_createFilterClausesAndAggregations_no_filters(self, testInstance, mocker):
        mockQuery = mocker.patch("api.elastic.Q")
        mockAgg = mocker.patch("api.elastic.A")

        testInstance.createFilterClausesAndAggregations([("showAll", "true")])

        mockQuery.assert_has_calls(
            [
                mocker.call("exists", field="editions.formats"),
            ]
        )
        mockAgg.assert_has_calls(
            [
                mocker.call("filter", exists={"field": "editions.formats"}),
            ]
        )

        assert testInstance.appliedFilters == []
        assert testInstance.appliedAggregations == []

    def test_addFiltersAndAggregations(self, testInstance, mocker):
        mockFilters = mocker.patch.object(ElasticClient, "applyFilters")
        mockAggregations = mocker.patch.object(ElasticClient, "applyAggregations")

        testInstance.addFiltersAndAggregations(1)

        mockFilters.assert_called_once_with(size=1)
        mockAggregations.assert_called_once()

    def test_geneateDateRange_start_end(self):
        testRange = ElasticClient.generateDateRange(
            [("startYear", 1900), ("endYear", 2000)]
        )

        assert testRange["gte"] == 1900
        assert testRange["lte"] == 2000

    def test_geneateDateRange_start_only(self):
        testRange = ElasticClient.generateDateRange([("startYear", 1900)])

        assert testRange["gte"] == 1900
        assert "lte" not in testRange.keys()

    def test_geneateDateRange_end_only(self):
        testRange = ElasticClient.generateDateRange([("endYear", 2000)])

        assert testRange["lte"] == 2000
        assert "gte" not in testRange.keys()

    def test_applyFilters_no_language_filters(self, testInstance, mocker):
        mockClause = mocker.patch("api.elastic.Q")
        mockClause.return_value = "filterQuery"

        mockQuery = mocker.MagicMock()
        mockQuery.query.return_value = mockQuery
        testInstance.query = mockQuery
        testInstance.dateSort = None
        testInstance.govDocFilter = None
        testInstance.appliedFilters = ["filter1", "filter2"]
        testInstance.languageFilters = []

        testInstance.applyFilters()

        mockClause.assert_called_once_with("bool", must=["filter1", "filter2"])
        testInstance.query.query.assert_called_once_with(
            "nested", path="editions", inner_hits={"size": 100}, query="filterQuery"
        )

    def test_applyFilters_language_filter_only(self, testInstance, mocker):
        mockClause = mocker.patch("api.elastic.Q")
        mockClause.side_effect = [
            "Lang1Filter",
            "Lang1Nested",
            "Lang2Filter",
            "Lang2Nested",
        ]

        mockQuery = mocker.MagicMock()
        mockQuery.query.return_value = mockQuery
        testInstance.query = mockQuery
        testInstance.dateSort = None
        testInstance.govDocFilter = None
        testInstance.appliedFilters = []
        testInstance.languageFilters = ["Lang1", "Lang2"]

        testInstance.applyFilters()

        mockClause.assert_has_calls(
            [
                mocker.call("bool", must=["Lang1"]),
                mocker.call(
                    "nested",
                    path="editions",
                    inner_hits={"size": 100},
                    query="Lang1Filter",
                ),
                mocker.call("bool", must=["Lang2"]),
                mocker.call("nested", path="editions", query="Lang2Filter"),
            ]
        )
        mockQuery.query.assert_called_once_with(
            "bool", must=["Lang1Nested", "Lang2Nested"]
        )

    def test_applyFilters_govDoc_filter_only(self, testInstance):
        testInstance.query = Search().query(Q())
        testInstance.dateSort = None
        testInstance.languageFilters = []
        testInstance.appliedFilters = []
        testInstance.govDocFilter = Q("term", **{"is_government_document": True})

        testInstance.applyFilters()

        assert testInstance.query.to_dict()["query"]["bool"]["must"][0] == {
            "nested": {
                "inner_hits": {"size": 100},
                "path": "editions",
                "query": {"match_all": {}},
            }
        }
        assert testInstance.query.to_dict()["query"]["bool"]["must"][1] == {
            "term": {"is_government_document": True}
        }

    def test_applyFilters_govDoc_and_other_filters(self, testInstance):
        testInstance.query = Search().query(Q())
        testInstance.dateSort = None
        testInstance.languageFilters = []
        testInstance.appliedFilters = [Q("term", editions__formats="testFormat")]
        testInstance.govDocFilter = Q("term", **{"is_government_document": True})

        testInstance.applyFilters()

        assert testInstance.query.to_dict()["query"]["bool"]["must"][0] == {
            "nested": {
                "inner_hits": {"size": 100},
                "path": "editions",
                "query": {
                    "bool": {"must": [{"term": {"editions.formats": "testFormat"}}]}
                },
            }
        }
        assert testInstance.query.to_dict()["query"]["bool"]["must"][1] == {
            "term": {"is_government_document": True}
        }

    def test_applyFilters_language_and_other_filters(self, testInstance, mocker):
        mockClause = mocker.patch("api.elastic.Q")
        mockClause.side_effect = ["Lang1Filter", "Lang1Nested"]

        mockQuery = mocker.MagicMock()
        mockQuery.query.return_value = mockQuery
        testInstance.query = mockQuery
        testInstance.dateSort = None
        testInstance.govDocFilter = None
        testInstance.appliedFilters = ["formatFilter"]
        testInstance.languageFilters = ["Lang1"]

        testInstance.applyFilters()

        mockClause.assert_has_calls(
            [
                mocker.call("bool", must=["formatFilter", "Lang1"]),
                mocker.call(
                    "nested",
                    path="editions",
                    inner_hits={"size": 100},
                    query="Lang1Filter",
                ),
            ]
        )
        mockQuery.query.assert_called_once_with("bool", must=["Lang1Nested"])

    def test_applyFilters_no_filters_date_ort(self, testInstance, mocker):
        mockClause = mocker.patch("api.elastic.Q")
        mockClause.return_value = "filterQuery"

        mockQuery = mocker.MagicMock()
        mockQuery.query.return_value = mockQuery
        testInstance.query = mockQuery
        testInstance.dateSort = {
            "editions.publication_date": {"test": "value", "nested": "toDelete"}
        }
        testInstance.govDocFilter = None
        testInstance.appliedFilters = []
        testInstance.languageFilters = []

        testInstance.applyFilters()

        mockClause.assert_called_once_with("match_all")
        mockQuery.query.assert_called_once_with(
            "nested",
            path="editions",
            inner_hits={
                "size": 100,
                "sort": {"editions.publication_date": {"test": "value"}},
            },
            query="filterQuery",
        )

    def test_applyAggregation_govDoc_only(self, testInstance):
        testInstance.query = Search()
        testInstance.dateSort = None
        testInstance.displayAggregation = None
        testInstance.appliedAggregations = []
        testInstance.govDocAgg = A(
            "filter", **{"term": {"is_government_document": True}}
        )

        testInstance.applyAggregations()

        assert testInstance.query.aggs.to_dict()["aggs"]["govDoc"] == {
            "terms": {"field": "is_government_document", "size": 2}
        }
        assert testInstance.query.aggs.to_dict()["aggs"]["govDoc_filter"] == {
            "filter": {"term": {"is_government_document": True}}
        }

    def test_applyAggregations_govDoc_and_other_aggregations(self, testInstance):
        testInstance.query = Search()
        testInstance.dateSort = None
        testInstance.displayAggregation = None
        testInstance.appliedAggregations = [
            A("filter", **{"range": {"editions.publication_date": "testDate"}}),
            A("filter", **{"terms": {"editions.formats": "testFormat"}}),
            A("filter", **{"exists": {"field": "editions.formats"}}),
        ]
        testInstance.govDocAgg = A(
            "filter", **{"term": {"is_government_document": False}}
        )

        testInstance.applyAggregations()

        assert testInstance.query.aggs.to_dict()["aggs"]["govDoc"] == {
            "terms": {"field": "is_government_document", "size": 2}
        }
        assert testInstance.query.aggs.to_dict()["aggs"]["govDoc_filter"] == {
            "filter": {"term": {"is_government_document": False}}
        }
        assert testInstance.query.aggs.to_dict()["aggs"]["editions"] == {
            "aggs": {
                "edition_filter_0": {
                    "aggs": {
                        "edition_filter_1": {
                            "aggs": {
                                "edition_filter_2": {
                                    "aggs": {
                                        "formats": {
                                            "aggs": {
                                                "editions_per": {"reverse_nested": {}}
                                            },
                                            "terms": {
                                                "field": "editions.formats",
                                                "size": 10,
                                            },
                                        },
                                        "lang_parent": {
                                            "aggs": {
                                                "languages": {
                                                    "aggs": {
                                                        "editions_per": {
                                                            "reverse_nested": {}
                                                        }
                                                    },
                                                    "terms": {
                                                        "field": "editions.languages.language",
                                                        "size": 200,
                                                    },
                                                }
                                            },
                                            "nested": {"path": "editions.languages"},
                                        },
                                    },
                                    "filter": {"exists": {"field": "editions.formats"}},
                                }
                            },
                            "filter": {"terms": {"editions.formats": "testFormat"}},
                        }
                    },
                    "filter": {"range": {"editions.publication_date": "testDate"}},
                }
            },
            "nested": {"path": "editions"},
        }

    def test_addSearchHighlighting(self, testInstance, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.highlight_options.return_value = mockQuery
        mockQuery.highlight.return_value = "testHighlightedQuery"

        testInstance.query = mockQuery

        testInstance.addSearchHighlighting()

        mockQuery.highlight_options.assert_called_once_with(
            order="score", number_of_fragments=10, pre_tags="", post_tags=""
        )

        mockQuery.highlight.assert_called_once()

        assert testInstance.query == "testHighlightedQuery"
