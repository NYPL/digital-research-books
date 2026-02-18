from base64 import b64decode
from functools import wraps
import os

from flask import current_app, request

from utils.common import require_env

from .db import DBClient
from .utils import APIUtils

from logger import create_log


logger = create_log(__name__)


def deprecated(message):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            resp = func(*args, **kwargs)
            return warn_deprecated(resp, message)

        return wrapper

    return decorator


def warn_deprecated(response, message):
    if isinstance(response, tuple):
        response[0].headers["Warning"] = message
        logger.warning(message)
    return response


def require_basic_authentication(func):
    @wraps(func)
    def decorator(*args, **kwargs):
        # Only printing header keys to avoid writing passwords to logs
        logger.debug(
            f"Attempting basic authentication with request headers: {list(request.headers.keys())}"
        )

        headers = {k.lower(): v for k, v in request.headers.items()}

        try:
            _, loginPair = headers["authorization"].split(" ")
            loginBytes = loginPair.encode("utf-8")
            username, password = b64decode(loginBytes).decode("utf-8").split(":")
        except KeyError:
            return APIUtils.formatResponseObject(
                403, "authResponse", {"message": "user/password not provided"}
            )

        db_client = DBClient(current_app.config["DB_CLIENT"])
        db_client.createSession()

        user = db_client.fetchUser(username)
        if user is None:
            logger.debug(
                f"User {username} not found in {os.environ.get('ENVIRONMENT')} database"
            )

        if (
            not user
            or APIUtils.validatePassword(password, user.password, user.salt) is False
        ):
            return APIUtils.formatResponseObject(
                401, "authResponse", {"message": "invalid user/password"}
            )

        db_client.closeSession()

        kwargs["user"] = user.user

        return func(*args, **kwargs)

    return decorator
