import os
from functools import wraps
from flask import request, jsonify


def require_api_key(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        expected_key = os.getenv("VRA_API_KEY")
        key = request.headers.get("X-API-Key")

        if key is None or key != expected_key:
            return jsonify({"error": "Unauthorized"}), 401

        return func(*args, **kwargs)

    return wrapper
