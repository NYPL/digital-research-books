from functools import wraps
from logger import create_log
import logging

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
