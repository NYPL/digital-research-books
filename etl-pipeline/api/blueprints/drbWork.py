from flask import Blueprint, request, current_app
from ..db import DBClient
from ..utils import APIUtils
from ..validation_utils import is_valid_uuid
from logger import create_log
from ..decorators import deprecated

logger = create_log(__name__)

work = Blueprint("work", __name__, url_prefix="/work")
works = Blueprint("works", __name__, url_prefix="/works")


@work.route("/<uuid>", methods=["GET"])
@deprecated("This endpoint is deprecated please use /works/<uuid> instead.")
@works.route("/<uuid>", methods=["GET"])
def get_work(uuid):
    logger.info(f"Getting work with id {uuid}")
    response_type = "singleWork"

    if not is_valid_uuid(uuid):
        return APIUtils.formatResponseObject(
            400, response_type, {"message": f"Work id {uuid} is invalid"}
        )

    try:
        with DBClient(current_app.config["DB_CLIENT"]) as db_client:
            search_params = APIUtils.normalizeQueryParams(request.args)
            terms = {
                param: APIUtils.extractParamPairs(param, search_params)
                for param in ["filter"]
            }
            show_all = search_params.get("showAll", ["true"])[0].lower() != "false"
            reader_version = (
                search_params.get("readerVersion", [None])[0]
                or current_app.config["READER_VERSION"]
            )
            filtered_formats = APIUtils.formatFilters(terms)

            work = db_client.fetchSingleWork(uuid)

            if not work:
                return APIUtils.formatResponseObject(
                    404, response_type, {"message": f"No work found with id {uuid}"}
                )

            return APIUtils.formatResponseObject(
                200,
                response_type,
                APIUtils.formatWorkOutput(
                    work,
                    None,
                    showAll=show_all,
                    request=request,
                    dbClient=db_client,
                    formats=filtered_formats,
                    reader=reader_version,
                ),
            )
    except Exception:
        logger.exception(f"Unable to get work with id {uuid}")
        return APIUtils.formatResponseObject(
            500, response_type, {"message": f"Unable to get work with id {uuid}"}
        )
