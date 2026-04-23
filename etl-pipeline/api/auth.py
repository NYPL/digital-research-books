from functools import wraps
from flask import request, jsonify

from logger import create_log
from utils.common import require_env

logger = create_log(__name__)


def require_api_key(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        expected_key = require_env("VRA_API_KEY")
        key = request.headers.get("X-API-Key")

        if key is None:
            logger.warning("API key missing from request headers")
            return jsonify({"error": "Unauthorized"}), 401

        if key != expected_key:
            logger.warning("Invalid API key provided")
            return jsonify({"error": "Unauthorized"}), 401

        return func(*args, **kwargs)

    return wrapper
