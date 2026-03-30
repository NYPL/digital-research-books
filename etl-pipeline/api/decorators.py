from base64 import b64decode
from functools import wraps
import os

from flask import current_app, make_response, request

from utils.common import require_env

from .db import DBClient
from .utils import APIUtils

from logger import create_log
from uuid import uuid4

from .session_jwt import sign_session, verify_session


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


def require_session_jwt(func):
    @wraps(func)
    def decorator(*args, **kwargs):
        is_dev = os.environ.get("ENVIRONMENT") == "local"
        cookie_name = os.environ.get("SESSION_COOKIE_NAME", "vra_session")

        token = request.cookies.get(cookie_name)

        generated_token = None
        session_uuid = None
        if token:
            try:
                session_uuid = verify_session(token)
            except Exception:
                return APIUtils.formatResponseObject(
                    401, "sessionResponse", {"message": "Invalid session"}
                )
        else:
            new_uuid = str(uuid4())
            try:
                generated_token = sign_session(new_uuid)
            except Exception:
                logger.exception("Failed to sign new session token")
                return APIUtils.formatResponseObject(
                    500, "sessionResponse", {"message": "Failed to create session"}
                )
            session_uuid = new_uuid

        kwargs["session_id"] = session_uuid

        response = func(*args, **kwargs)

        if generated_token:
            try:
                resp_obj = make_response(response)
                resp_obj.set_cookie(
                    cookie_name,
                    generated_token,
                    httponly=True,
                    secure=not is_dev,
                    samesite="Lax",
                    path="/",
                )
            except Exception:
                logger.exception("Unable to set session cookie on response")

        return response

    return decorator
