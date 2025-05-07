import os

import jwt

from flask import Blueprint, request, redirect, current_app
from ..db import DBClient
from ..utils import APIUtils
from ..validation_utils import is_valid_numeric_id
from managers import S3Manager
from logger import create_log

logger = create_log(__name__)

fulfill = Blueprint("fulfill", __name__, url_prefix="/fulfill")
response_type = "fulfill"


@fulfill.route("/<link_id>", methods=["GET"])
def fulfill_item(link_id):
    logger.info(f"Fulfilling item for link id {link_id}")

    if not is_valid_numeric_id(link_id):
        return APIUtils.formatResponseObject(
            400, response_type, {"message": f"Link id {link_id} is invalid"}
        )

    try:
        with DBClient(current_app.config["DB_CLIENT"]) as db_client:
            link = db_client.fetchSingleLink(link_id)

        if not link:
            return APIUtils.formatResponseObject(
                404, response_type, {"message": f"No link found with id {link_id}"}
            )

        requires_authorization = (
            link.flags.get("edd", False) is False and link.flags["nypl_login"] is True
        )

        if not requires_authorization:
            # TODO: Redirect to the link url if it doesn't require authorization
            return APIUtils.formatResponseObject(
                400,
                response_type,
                {"message": f"Link id {link_id} does not require authorization"},
            )

        if requires_authorization:
            return redirect_to_link_url(link.url)
    except Exception:
        logger.exception(f"Unable to fulfill link with id {link_id}")
        return APIUtils.formatResponseObject(
            500, response_type, {"message": f"Unable to fulfill link with id {link_id}"}
        )


def redirect_to_link_url(link_url: str):
    decoded_token = None

    try:
        bearer = request.headers.get("Authorization")

        if bearer is None:
            return APIUtils.formatResponseObject(
                401,
                response_type,
                "Invalid access token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        token = bearer.split()[1]
        jwt_secret = os.environ["NYPL_API_CLIENT_PUBLIC_KEY"]

        decoded_token = jwt.decode(
            token, jwt_secret, algorithms=["RS256"], audience="app_myaccount"
        )
    except jwt.exceptions.ExpiredSignatureError:
        return APIUtils.formatResponseObject(
            401,
            response_type,
            "Expired access token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except (
        jwt.exceptions.DecodeError,
        UnicodeDecodeError,
        IndexError,
        AttributeError,
    ):
        return APIUtils.formatResponseObject(
            401,
            response_type,
            "Invalid access token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if decoded_token.get("iss", None) == "https://www.nypl.org":
        s3_manager = S3Manager()
        presigned_get_url = APIUtils.getPresignedUrlFromObjectUrl(
            s3_manager.client, link_url
        )

        return redirect(presigned_get_url)

    return APIUtils.formatResponseObject(
        401,
        response_type,
        "Invalid access token",
        headers={"WWW-Authenticate": "Bearer"},
    )
