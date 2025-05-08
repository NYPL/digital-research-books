from copy import deepcopy
from elasticsearch_dsl import Search, Q, A
from hashlib import sha1
import os
import re

from .utils import APIUtils
from logger import create_log

logger = create_log(__name__)


class ElasticClient:
    ROLE_ALLOWLIST = [
        "author of afterword, colophon, etc.",
        "author of dialog",
        "author of introduction, etc.",
        "author",
        "colorist",
        "composer",
        "compiler",
        "creator",
        "contributor",
        "editor",
        "film director",
        "film producer",
        "illustrator",
        "illuminator",
        "interviewer",
        "interviewee",
        "lyricist",
        "translator",
        "videographer",
        "writer of introduction",
        "writer of preface",
        "writer of supplementary textual content",
    ]

    def __init__(self, redisClient):
        self.environment = os.environ["ENVIRONMENT"]
        self.esIndex = os.environ["ELASTICSEARCH_INDEX"]

        self.redis = redisClient

        self.dateSort = None
        self.sortReversed = False

        self.languageFilters = []
        self.govDocFilter = None
        self.govDocAgg = None
        self.appliedFilters = []

        self.appliedAggregations = []
        self.searchedFields = []

    def createSearch(self):
        s = Search(index=os.environ["ELASTICSEARCH_INDEX"])
        searchES = s.params(track_total_hits=True)
        return searchES

    def searchQuery(self, params, page=0, perPage=10):
        self.generateSearchQuery(params)

        return self.executeSearchQuery(params, page, perPage)

    def generateSearchQuery(self, params):
        authorityList = [
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
        ]

        search = self.createSearch()
        search.source(["uuid", "editions"])

        searchClauses = []
        for field, query in params["query"]:
            escapedQuery = self.escapeSearchQuery(query)

            if field is None or field in ["keyword", "query"]:
                searchClauses.append(
                    Q(
                        "bool",
                        should=[
                            self.titleQuery(escapedQuery),
                            self.authorQuery(escapedQuery),
                            self.subjectQuery(escapedQuery),
                        ],
                    )
                )
            elif field == "title":
                searchClauses.append(self.titleQuery(escapedQuery))
            elif field == "author":
                searchClauses.append(self.authorQuery(escapedQuery))
            elif field == "subject":
                searchClauses.append(self.subjectQuery(escapedQuery))
            elif field == "viaf" or field == "lcnaf":
                searchClauses.append(self.authorityQuery(field, query))
            elif field == "identifier":
                searchClauses.append(self.identifierQuery(authorityList, query))
            else:
                searchClauses.append(Q("match", **{field: escapedQuery}))

        self.query = search.query(Q("bool", must=searchClauses))

        self.createFilterClausesAndAggregations(params["filter"])

        self.addSortClause(params["sort"])
        self.addFiltersAndAggregations(3)
        self.addSearchHighlighting()

    def executeSearchQuery(self, params, page, perPage):
        startPos, endPos = ElasticClient.getFromSize(page, perPage)

        queryHash = self.generateQueryHash(params, startPos)

        searchFromStr = self.getPageResultCache(queryHash) if startPos > 0 else None

        if searchFromStr:
            logger.info("Found cached search page: {}".format(searchFromStr))

            searchFrom = list(searchFromStr.decode().split("|"))

            res = self.query.extra(search_after=searchFrom)[0:perPage].execute()
        elif startPos > 5000:
            totalCount = self.query.count()
            if totalCount - startPos < 10000:
                logger.debug("Executing Reversed Search")

                res = self.executeReversedQuery(params, totalCount, startPos, perPage)
            else:
                logger.debug("Executing Deep Pagination Search")
                res = self.executeDeepQuery(startPos, perPage)
        else:
            res = self.query[startPos:endPos].execute()

        if not searchFromStr:
            try:
                lastSort = list(res.hits[-1].meta.sort)
                self.setPageResultCache(queryHash, lastSort)
            except IndexError:
                logger.debug("Empty result set, skipping paging cache")

        return res

    def executeReversedQuery(self, params, totalCount, startPos, perPage):
        self.sortReversed = True
        self.query = self.query.sort()  # Clear existing sort
        self.addSortClause(params["sort"], reverse=True)

        revEndPos = totalCount - startPos
        revStartPos = revEndPos - perPage if revEndPos > 10 else 0

        return self.query[revStartPos:revEndPos].execute()

    def executeDeepQuery(self, startPos, perPage):
        self.query = self.query.source(False)

        pagingEnd = 5000
        iteration = 1
        while True:
            pagingResult = self.query[0:pagingEnd].execute()
            lastSort = list(pagingResult.hits[-1].meta.sort)

            self.query = self.query.extra(search_after=lastSort)

            if pagingEnd < 5000:
                self.query = self.query.source(True)
                return self.query[0:perPage].execute()
            elif startPos - (pagingEnd * iteration) < 5000:
                pagingEnd = startPos % 5000

            iteration += 1

    def setPageResultCache(self, cacheKey, sort):
        self.redis.set(
            "{}/queryPaging/{}".format(self.environment, cacheKey),
            "|".join([str(s) for s in sort]),
            ex=60 * 60 * 24,
        )

    def getPageResultCache(self, cacheKey):
        return self.redis.get("{}/queryPaging/{}".format(self.environment, cacheKey))

    @classmethod
    def generateQueryHash(cls, params, startPos):
        hashDict = deepcopy(params)
        hashDict["position"] = startPos

        hashableDict = cls.makeDictHashable(hashDict)

        hasher = sha1()
        hasher.update(repr(hashableDict).encode())

        return hasher.hexdigest()

    @classmethod
    def makeDictHashable(cls, object):
        if isinstance(object, dict):
            return tuple(
                sorted((k, cls.makeDictHashable(v)) for k, v in object.items())
            )
        elif isinstance(object, (list, tuple, set)):
            return tuple([cls.makeDictHashable(sub) for sub in object])

        return object

    @staticmethod
    def escapeSearchQuery(query):
        return re.sub(r"[\*\+\-\&\|\!\(\)\[\]\{\}\^\~\?\:\\\/]{1}", "\\\\\g<0>", query)

    def languageQuery(self, workTotals):
        search = self.createSearch()

        query = search.query(Q())[:0]

        languageAgg = query.aggs.bucket(
            "languages", A("nested", path="editions.languages")
        ).bucket(
            "languages",
            "terms",
            **{"field": "editions.languages.language", "size": 250},
        )

        if workTotals:
            languageAgg.bucket("work_totals", "reverse_nested")

        return query.execute()

    def titleQuery(self, titleText):
        self.searchedFields.extend(["title", "alt_titles", "editions.title"])

        return Q(
            "bool",
            should=[
                Q(
                    "query_string",
                    query=titleText,
                    fields=["title.*^3", "alt_titles.*"],
                    default_operator="and",
                ),
                Q(
                    "nested",
                    path="editions",
                    query=Q(
                        "query_string",
                        query=titleText,
                        fields=["editions.title.*"],
                        default_operator="and",
                    ),
                ),
            ],
        )

    # Query for the identifier authority followed up with the identifier value
    def identifierQuery(self, authorityList, authIdentText):
        self.searchedFields.extend(
            [
                "identifiers.identifier",
                "editions.identifiers.identifier",
                "identifiers.authority",
                "editions.identifiers.authority",
            ]
        )

        # User will type in authority|identifier in the url
        authIdentList = authIdentText.split("|")

        # When user only types in the identifier in the search bar
        if len(authIdentList) < 2:
            identifier = authIdentText

            workIdentQuery = Q(
                "bool", must=[Q("term", **{"identifiers.identifier": identifier})]
            )

            editionIdentQuery = Q(
                "bool",
                must=[Q("term", **{"editions.identifiers.identifier": identifier})],
            )

            return Q(
                "bool",
                should=[
                    Q("nested", path="identifiers", query=workIdentQuery),
                    Q("nested", path="editions.identifiers", query=editionIdentQuery),
                ],
            )

        # When user types in the authority and identifier in the search bar
        else:
            authority = authIdentList[0]
            identifier = authIdentList[1]

            if authority in authorityList:
                workIdentAuthQuery = Q(
                    "bool",
                    must=[
                        Q("term", **{"identifiers.authority": authority}),
                        Q("term", **{"identifiers.identifier": identifier}),
                    ],
                )

                editionIdentAuthQuery = Q(
                    "bool",
                    must=[
                        Q("term", **{"editions.identifiers.authority": authority}),
                        Q("term", **{"editions.identifiers.identifier": identifier}),
                    ],
                )

                return Q(
                    "bool",
                    should=[
                        Q("nested", path="identifiers", query=workIdentAuthQuery),
                        Q(
                            "nested",
                            path="editions.identifiers",
                            query=editionIdentAuthQuery,
                        ),
                    ],
                )

    def authorQuery(self, authorText):
        self.searchedFields.extend(["agents.name", "editions.agents.name"])

        workAgentQuery = Q(
            "bool",
            must=[
                Q(
                    "query_string",
                    query=authorText,
                    fields=["agents.name^2"],
                    default_operator="and",
                ),
                Q("terms", agents__roles=self.ROLE_ALLOWLIST),
            ],
        )

        editionAgentQuery = Q(
            "bool",
            must=[
                Q(
                    "query_string",
                    query=authorText,
                    fields=["editions.agents.name"],
                    default_operator="and",
                ),
                Q("terms", editions__agents__roles=self.ROLE_ALLOWLIST),
            ],
        )

        return Q(
            "bool",
            should=[
                Q("nested", path="agents", query=workAgentQuery),
                Q("nested", path="editions.agents", query=editionAgentQuery),
            ],
        )

    def authorityQuery(self, authority, authorityID):
        workAgentField = "agents.{}".format(authority)
        editionAgentField = "editions.agents.{}".format(authority)

        self.searchedFields.extend([workAgentField, editionAgentField])

        workAuthorityQuery = Q("term", **{workAgentField: authorityID})
        editionAuthorityQuery = Q("term", **{editionAgentField: authorityID})

        return Q(
            "bool",
            should=[
                Q("nested", path="agents", query=workAuthorityQuery),
                Q("nested", path="editions.agents", query=editionAuthorityQuery),
            ],
        )

    def subjectQuery(self, subjectText):
        self.searchedFields.append("subjects.heading")

        return Q(
            "nested",
            path="subjects",
            query=Q(
                "query_string",
                query=subjectText,
                fields=["subjects.heading.*"],
                default_operator="and",
            ),
        )

    @staticmethod
    def getFromSize(page, perPage):
        startPos = page * perPage
        endPos = startPos + perPage
        return startPos, endPos

    def addSortClause(self, sortParams, reverse=False):
        sortValues = []
        for sort, direction in sortParams:
            sortDir = direction.lower() if direction else "asc"

            if reverse is True:
                sortDir = "desc" if sortDir == "asc" else "asc"

            if sort == "title":
                sortValues.append({"sort_title": {"order": sortDir}})
            elif sort == "author":
                sortValues.append(
                    {
                        "agents.sort_name": {
                            "order": sortDir,
                            "nested": {
                                "path": "agents",
                                "filter": {"terms": {"agents.roles": ["author"]}},
                                "max_children": 1,
                            },
                        }
                    }
                )
            elif sort == "date":
                sortFilters = [f.to_dict() for f in self.appliedFilters]

                if len(self.languageFilters) > 0:
                    sortFilters.extend(
                        [lang.to_dict() for lang in self.languageFilters]
                    )

                self.dateSort = {
                    "editions.publication_date": {
                        "order": sortDir,
                        "nested": {
                            "path": "editions",
                            "filter": {"bool": {"must": sortFilters}},
                        },
                    }
                }

                sortValues.append(self.dateSort)

        if len(sortValues) < 1:
            sortValues.append({"_score": "desc"})

        sortValues.append({"uuid": "asc" if reverse is False else "desc"})

        self.query = self.query.sort(*sortValues)

    def createFilterClausesAndAggregations(self, filterParams):
        dateFilters = list(filter(lambda x: "year" in x[0].lower(), filterParams))
        languageFilters = list(filter(lambda x: x[0] == "language", filterParams))
        formatFilters = list(filter(lambda x: x[0] == "format", filterParams))
        displayFilters = list(filter(lambda x: x[0] == "showAll", filterParams))
        govDocFilters = list(filter(lambda x: x[0] == "govDoc", filterParams))

        dateFilter, dateAggregation = (None, None)
        formatFilter, formatAggregation = (None, None)
        displayFilter, displayAggregation = (
            Q("exists", field="editions.formats"),
            A("filter", **{"exists": {"field": "editions.formats"}}),
        )

        if len(languageFilters) > 0:
            self.languageFilters = [
                Q(
                    "nested",
                    path="editions.languages",
                    query=Q("term", editions__languages__language=language[1]),
                )
                for language in languageFilters
            ]

        if len(dateFilters) > 0:
            dateRange = ElasticClient.generateDateRange(dateFilters)
            dateFilter = Q("range", **{"editions.publication_date": dateRange})
            dateAggregation = A(
                "filter", **{"range": {"editions.publication_date": dateRange}}
            )

        if len(formatFilters) > 0:
            formats = []
            for format in formatFilters:
                try:
                    formats.extend(APIUtils.FORMAT_CROSSWALK[format[1]])
                except KeyError:
                    raise ElasticClientError(
                        "Invalid format filter {} received".format(format[1])
                    )

            formatFilter = Q("terms", editions__formats=formats)
            formatAggregation = A("filter", **{"terms": {"editions.formats": formats}})

        if len(govDocFilters) > 0:
            if govDocFilters[0][1] == "noGovDoc":
                self.govDocFilter = Q("term", **{"is_government_document": False})
                self.govDocAgg = A(
                    "filter", **{"term": {"is_government_document": False}}
                )

            elif govDocFilters[0][1] == "onlyGovDoc":
                self.govDocFilter = Q("term", **{"is_government_document": True})
                self.govDocAgg = A(
                    "filter", **{"term": {"is_government_document": True}}
                )

        if len(displayFilters) > 0 and displayFilters[0][1] == "true":
            displayFilter = None
            displayAggregation = None

        self.appliedFilters = list(
            filter(None, [dateFilter, formatFilter, displayFilter])
        )

        self.appliedAggregations = list(
            filter(None, [dateAggregation, formatAggregation, displayAggregation])
        )

    def addFiltersAndAggregations(self, innerHits):
        self.applyFilters(size=innerHits)
        self.applyAggregations()

    @staticmethod
    def generateDateRange(dateFilters):
        filterRange = {}
        for field, value in dateFilters:
            if field == "startYear":
                filterRange["gte"] = value
            elif field == "endYear":
                filterRange["lte"] = value

        return filterRange

    def applyFilters(self, size=100):
        innerHitsClause = {"size": size}

        if self.dateSort:
            innerHitSort = deepcopy(self.dateSort)
            del innerHitSort["editions.publication_date"]["nested"]

            innerHitsClause["sort"] = innerHitSort

        if len(self.languageFilters) > 0:
            filters = []
            for i, langFilter in enumerate(self.languageFilters):
                filterSet = self.appliedFilters + [langFilter]
                if i == 0:
                    filters.append(
                        Q(
                            "nested",
                            path="editions",
                            inner_hits=innerHitsClause,
                            query=Q("bool", must=filterSet),
                        )
                    )
                else:
                    filters.append(
                        Q("nested", path="editions", query=Q("bool", must=filterSet))
                    )

            self.query = self.query.query("bool", must=filters)

        elif len(self.appliedFilters) > 0:
            self.query = self.query.query(
                "nested",
                path="editions",
                inner_hits=innerHitsClause,
                query=Q("bool", must=self.appliedFilters),
            )
        else:
            self.query = self.query.query(
                "nested",
                path="editions",
                inner_hits=innerHitsClause,
                query=Q("match_all"),
            )

        if self.govDocFilter != None:
            self.query = self.query.query(self.govDocFilter)

    def applyAggregations(self):
        rootAgg = self.query.aggs.bucket("editions", A("nested", path="editions"))

        lastAgg = rootAgg
        for i, agg in enumerate(self.appliedAggregations):
            currentAgg = "edition_filter_{}".format(i)
            lastAgg = lastAgg.bucket(currentAgg, agg)

        lastAgg.bucket("lang_parent", "nested", path="editions.languages").bucket(
            "languages",
            "terms",
            **{"field": "editions.languages.language", "size": 200},
        ).bucket("editions_per", "reverse_nested")

        lastAgg.bucket(
            "formats", "terms", **{"field": "editions.formats", "size": 10}
        ).bucket("editions_per", "reverse_nested")

        if self.govDocAgg != None:
            self.query.aggs.bucket("govDoc_filter", self.govDocAgg)
        self.query.aggs.bucket(
            "govDoc", "terms", **{"field": "is_government_document", "size": 2}
        )

    def addSearchHighlighting(self):
        self.query = self.query.highlight_options(
            order="score", number_of_fragments=10, pre_tags="", post_tags=""
        )

        self.query = self.query.highlight(*self.searchedFields)


class ElasticClientError(Exception):
    pass
