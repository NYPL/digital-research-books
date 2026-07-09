from functools import wraps
from flask import request

from logger import create_log
from utils.common import require_env

from .utils import APIUtils

logger = create_log(__name__)

RESPONSE_TYPE = "auth"


def require_api_key(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        expected_key = require_env("VRA_API_KEY")
        key = request.headers.get("X-API-Key")

        if key is None:
            logger.warning("API key missing from request headers")
            return APIUtils.formatResponseObject(
                401, RESPONSE_TYPE, {"message": "Unauthorized"}
            )

        if key != expected_key:
            logger.warning("Invalid API key provided")
            return APIUtils.formatResponseObject(
                401, RESPONSE_TYPE, {"message": "Unauthorized"}
            )

        return func(*args, **kwargs)

    return wrapper
