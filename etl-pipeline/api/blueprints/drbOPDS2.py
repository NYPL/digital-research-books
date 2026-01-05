from flask import Blueprint, current_app, request
from ..db import DBClient
from ..elastic import ElasticClient

from ..opdsUtils import OPDSUtils
from ..utils import APIUtils
from ..opds2 import Feed, Link, Metadata, Navigation, Publication, Facet, Group
from logger import create_log

logger = create_log(__name__)


opds = Blueprint("opds", __name__, url_prefix="/opds")


@opds.route("/", methods=["GET"])
def opdsRoot():
    logger.info("Returning Root OPDS2 feed")

    rootFeed = constructBaseFeed(request.path, "Digital Research Books Home")

    return APIUtils.formatOPDS2Object(200, rootFeed)


@opds.route("/new", methods=["GET"])
def newPublications():
    logger.info("Returning OPDS2 feed of new publications")

    params = APIUtils.normalizeQueryParams(request.args)
    page = int(params.get("page", [1])[0]) - 1
    pageSize = int(params.get("size", [25])[0])

    dbClient = DBClient(current_app.config["DB_CLIENT"])
    dbClient.createSession()

    baseFeed = constructBaseFeed(
        request.full_path, "New Publications: Digital Research Books", grouped=True
    )

    pubCount, newPubs = dbClient.fetchNewWorks(page=page, size=pageSize)

    OPDSUtils.addPagingOptions(
        baseFeed, request.full_path, pubCount, page=page + 1, perPage=pageSize
    )

    addPublications(baseFeed, newPubs, grouped=True)

    dbClient.closeSession()

    return APIUtils.formatOPDS2Object(200, baseFeed)


@opds.route("/search", methods=["GET"])
def opdsSearch():
    logger.info("Returning OPDS2 feed of new publications")

    params = APIUtils.normalizeQueryParams(request.args)
    page = int(params.get("page", [1])[0]) - 1
    pageSize = int(params.get("size", [25])[0])

    searchTerms = {"query": [], "filter": [], "sort": []}
    for queryField in ["keyword", "title", "author", "subject"]:
        searchTerms["query"].extend(
            [(queryField, term) for term in params.get(queryField, [])]
        )

    searchTerms["filter"] = APIUtils.extractParamPairs("filter", params)
    if params.get("showAll", None):
        searchTerms["filter"].append(("showAll", params["showAll"][0]))

    esClient = ElasticClient(current_app.config["REDIS_CLIENT"])
    dbClient = DBClient(current_app.config["DB_CLIENT"])
    dbClient.createSession()

    logger.info("Executing ES Query {}".format(searchTerms))

    searchResult = esClient.searchQuery(searchTerms, page=page, perPage=pageSize)

    results = []
    highlights = {}
    for res in searchResult.hits:
        editionIds = [e.edition_id for e in res.meta.inner_hits.editions.hits]

        if getattr(res.meta, "highlight", None):
            highlights[res.uuid] = {
                key: list(set(res.meta.highlight[key])) for key in res.meta.highlight
            }

        results.append((res.uuid, editionIds))

    works = dbClient.fetchSearchedWorks(results)

    searchFeed = constructBaseFeed(request.full_path, "Search Results", grouped=True)

    OPDSUtils.addPagingOptions(
        searchFeed,
        request.full_path,
        searchResult.hits.total.value,
        page=page + 1,
        perPage=pageSize,
    )

    addFacets(searchFeed, request.full_path, searchResult.aggregations.to_dict())

    addPublications(searchFeed, works, grouped=True, highlights=highlights)

    dbClient.closeSession()

    return APIUtils.formatOPDS2Object(200, searchFeed)


@opds.route("/publication/<uuid>", methods=["GET"])
def fetchPublication(uuid):
    logger.info("Returning OPDS2 publication for {}".format(uuid))

    dbClient = DBClient(current_app.config["DB_CLIENT"])
    dbClient.createSession()

    workRecord = dbClient.fetchSingleWork(uuid)

    if workRecord is None:
        return APIUtils.formatResponseObject(
            404,
            "opdsPublication",
            {"message": "Unable to find work for uuid {}".format(uuid)},
        )

    publication = createPublicationObject(workRecord, searchResult=False)

    publication.addLink(
        {
            "rel": "search",
            "href": "/opds/search{?query,title,subject,author}",
            "type": "application/opds+json",
            "templated": True,
        }
    )

    dbClient.closeSession()

    return APIUtils.formatOPDS2Object(200, publication)


def constructBaseFeed(path, title, grouped=False):
    feed = Feed()

    feedMetadata = Metadata(title=title)
    feed.addMetadata(feedMetadata)

    selfLink = Link(rel="self", href=path, type="application/opds+json")
    searchLink = Link(
        rel="search",
        href="/opds/search{?query,title,subject,author}",
        type="application/opds+json",
        templated=True,
    )
    altLink = Link(rel="alternative", href="/", type="text/html")

    feed.addLinks([selfLink, searchLink, altLink])

    currentNavigation = Navigation(
        href=path, title=title, type="application/opds+json", rel="current"
    )

    navOptions = [currentNavigation]

    if path != "/opds/":
        baseNavigation = Navigation(
            href="/opds", title="Home", type="application/opds+json", rel="home"
        )
        navOptions.append(baseNavigation)

    if path != "/opds/new/":
        newNavigation = Navigation(
            href="/opds/new",
            title="New Works",
            type="application/opds+json",
            rel="http://opds-spec.org/sort/new",
        )
        navOptions.append(newNavigation)

    if grouped is True:
        navGroup = Group(metadata={"title": "Main Menu"})
        navGroup.addNavigations(navOptions)
        feed.addGroup(navGroup)
    else:
        feed.addNavigations(navOptions)

    return feed


def addPublications(feed, publications, grouped=False, highlights={}):
    print(publications)
    opdsPubs = [
        createPublicationObject(
            pub, _meta={"highlights": highlights.get(str(pub.uuid), {})}
        )
        for pub in publications
    ]

    if grouped is True:
        pubGroup = Group(metadata={"title": "Publications"})
        pubGroup.addPublications(opdsPubs)
        feed.addGroup(pubGroup)
    else:
        feed.addPublications(opdsPubs)


def createPublicationObject(publication, searchResult=True, _meta={}):
    newPub = Publication(metadata={"_meta": _meta})
    newPub.parseWorkToPublication(publication, searchResult=searchResult)

    return newPub


def addFacets(feed, path, facets):
    reducedFacets = APIUtils.formatAggregationResult(facets)

    opdsFacets = []

    for facet, options in reducedFacets.items():
        newFacet = Facet(metadata={"title": facet})

        facetOptions = [
            {
                "href": "{}&filter={}:{}".format(path, facet[:-1], option["value"]),
                "type": "application/opds+json",
                "title": option["value"],
                "properties": {"numberOfItems": option["count"]},
            }
            for option in options
        ]

        newFacet.addLinks(facetOptions)

        opdsFacets.append(newFacet)

    opdsFacets.append(
        Facet(
            metadata={"title": "Show All Editions"},
            links=[
                {
                    "href": "{}&showAll=true".format(path),
                    "type": "application/opds+json",
                    "title": "True",
                },
                {
                    "href": "{}&showAll=false".format(path),
                    "type": "application/opds+json",
                    "title": "False",
                },
            ],
        )
    )

    feed.addFacets(opdsFacets)
