from flask import Blueprint, current_app, request, Response
from flask_cors import cross_origin
import os
import requests
from urllib.parse import unquote_plus, urlparse

from ..db import DBClient
from ..elastic import ElasticClient
from ..utils import APIUtils
from logger import create_log

logger = create_log(__name__)


utils = Blueprint("utils", __name__, url_prefix="/utils")
EXCLUDED_HEADERS = [
    "content-encoding",
    "content-length",
    "transfer-encoding",
    "x-frame-options",
    "referrer-policy",
    "access-control-allow-origin",
    "connection",
    "keep-alive",
    "public",
    "proxy-authenticate",
    "upgrade",
]


@utils.route("/languages", methods=["GET"])
def get_languages():
    response_type = "languageCounts"

    try:
        es_client = ElasticClient(current_app.config["REDIS_CLIENT"])

        request_params = APIUtils.normalizeQueryParams(request.args)
        work_counts = request_params.get("totals", ["true"])[0].lower() != "false"

        language_query_results = es_client.languageQuery(work_counts)

        return APIUtils.formatResponseObject(
            200,
            response_type,
            APIUtils.formatLanguages(language_query_results.aggregations, work_counts),
        )
    except Exception:
        logger.exception("Unable to get language counts")
        return APIUtils.formatResponseObject(
            500, response_type, "Unable to get language counts"
        )


@utils.route("/counts", methods=["GET"])
def get_counts():
    response_type = "totalCounts"

    try:
        with DBClient(current_app.config["DB_CLIENT"]) as db_client:
            total_counts = db_client.fetchRowCounts()

        return APIUtils.formatResponseObject(
            200, response_type, APIUtils.formatTotals(total_counts)
        )
    except Exception:
        logger.exception("Unable to get total counts")
        return APIUtils.formatResponseObject(
            500, response_type, "Unable to get total counts"
        )


@utils.route("/proxy", methods=["GET", "POST", "PUT", "HEAD", "OPTIONS"])
@cross_origin(origins=os.environ.get("API_PROXY_CORS_ALLOWED", "*"))
def proxy_response():
    try:
        proxy_url = request.args.get("proxy_url")
        clean_url = unquote_plus(proxy_url)
        url_parts = urlparse(clean_url)

        logger.info(f"Proxying {request.method} request to {proxy_url}")

        while True:
            head_resp = requests.head(clean_url, headers={"User-agent": "Mozilla/5.0"})

            status_code = head_resp.status_code

            logger.info(
                f"HEAD response from {clean_url} returned status code {status_code}"
            )

            if status_code in [200, 204]:
                break
            elif status_code in [301, 302, 303, 307, 308]:
                clean_url = head_resp.headers["Location"]

                if clean_url[0] == "/":
                    clean_url = "{}://{}{}".format(
                        url_parts.scheme, url_parts.netloc, clean_url
                    )
            else:
                logger.warn(f"Unable to proxy clean url {clean_url}")
                clean_url = proxy_url
                break

        response = requests.request(
            method=request.method,
            url=clean_url,
            headers={k: v for (k, v) in request.headers if k != "Host"},
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False,
        )

        headers = [
            (k, v)
            for (k, v) in response.headers.items()
            if k.lower() not in EXCLUDED_HEADERS
        ]

        logger.info(
            f"Returning {request.method} response from {clean_url} with status code {response.status_code}"
        )

        return Response(response.content, response.status_code, headers)
    except Exception as e:
        logger.exception("Unable to proxy response")
        return Response("Unable to proxy response", 500)
