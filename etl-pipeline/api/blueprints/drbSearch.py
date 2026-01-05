from flask import Blueprint, request, current_app
from ..elastic import ElasticClient, ElasticClientError
from ..db import DBClient
from ..utils import APIUtils
from logger import create_log

logger = create_log(__name__)

search = Blueprint("search", __name__, url_prefix="/search")


@search.route("", methods=["GET"])
def query():
    response_type = "searchResponse"

    try:
        es_client = ElasticClient(current_app.config["REDIS_CLIENT"])
        db_client = DBClient(current_app.config["DB_CLIENT"])
        db_client.createSession()

        search_params = APIUtils.normalizeQueryParams(request.args)
        terms = {
            param: APIUtils.extractParamPairs(param, search_params)
            for param in ["query", "sort", "filter", "showAll"]
        }

        if terms.get("showAll", None):
            terms["filter"].append(terms["showAll"][0])
            del terms["showAll"]

        search_page = int(search_params.get("page", [1])[0]) - 1
        search_size = int(search_params.get("size", [10])[0])
        reader_version = (
            search_params.get("readerVersion", [None])[0]
            or current_app.config["READER_VERSION"]
        )

        logger.info(
            "Executing ES Query {} with filters {}".format(
                search_params, terms["filter"]
            )
        )

        try:
            search_result = es_client.searchQuery(
                terms, page=search_page, perPage=search_size
            )
        except ElasticClientError as e:
            logger.exception("Unable to execute search")
            return APIUtils.formatResponseObject(
                500, response_type, {"message": "Unable to execute search"}
            )

        results = []
        for res in search_result.hits:
            edition_ids = [e.edition_id for e in res.meta.inner_hits.editions.hits]

            try:
                highlights = {
                    key: list(set(res.meta.highlight[key]))
                    for key in res.meta.highlight
                }
            except AttributeError:
                highlights = {}

            results.append((res.uuid, edition_ids, highlights))

        if es_client.sortReversed is True:
            results = [r for r in reversed(results)]

        filtered_formats = APIUtils.formatFilters(terms)

        works = db_client.fetchSearchedWorks(results)

        # Depending on the version of elastic search, hits will either be an integer or a dictionary
        total_hits = (
            search_result.hits.total
            if isinstance(search_result.hits.total, int)
            else search_result.hits.total.value
        )

        facets = APIUtils.formatAggregationResult(search_result.aggregations.to_dict())
        paging = APIUtils.formatPagingOptions(search_page + 1, search_size, total_hits)

        data_block = {
            "totalWorks": total_hits,
            "works": APIUtils.formatWorkOutput(
                works,
                results,
                request=request,
                dbClient=db_client,
                formats=filtered_formats,
                reader=reader_version,
            ),
            "paging": paging,
            "facets": facets,
        }

        db_client.closeSession()

        return APIUtils.formatResponseObject(200, response_type, data_block)
    except Exception:
        logger.exception("Unable to execute search")
        return APIUtils.formatResponseObject(
            500, response_type, {"message": "Unable to execute search"}
        )
