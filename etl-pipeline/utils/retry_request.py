from requests import ConnectionError, Timeout
from typing import Callable

import functools
import time


def retry_request(
    max_retries: int = 3,
    wait_seconds: int = 60,
    log_func: Callable[[str], None] | None = None,
    retry_exceptions: tuple[type[Exception], ...] | None = None,
):
    """
    Exponential backoff retry decorator

    Params:
        max_retries: The number of attempts made to execute the function without
            error before raising the final error.
        wait_seconds: Base wait time (this is the time waited after the first error).
        log_func: A function to log before each retry. (e.g. `logger.info` or `print`)
        retry_exceptions: A tuple of exception types to catch and retry.
            Defaults to (ConnectionError, Timeout).
    """
    if retry_exceptions is None:
        retry_exceptions = (ConnectionError, Timeout)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_exceptions as e:
                    if attempt < max_retries:
                        sleep_time = wait_seconds * (2 ** (attempt - 1))
                        if log_func is not None:
                            log_func(
                                f"Retrying {func.__qualname__} after {sleep_time}s because attempt #{attempt} failed."
                            )
                        time.sleep(sleep_time)
                    else:
                        raise e
                except Exception as e:
                    raise e

        return wrapper

    return decorator
