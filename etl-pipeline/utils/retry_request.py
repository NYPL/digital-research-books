import functools
from requests import ConnectionError, Timeout
import time


def retry_request(max_retries: int = 3, wait_seconds: int = 60):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (ConnectionError, Timeout) as e:
                    if attempt < max_retries - 1:
                        time.sleep(wait_seconds * (attempt + 1))
                    else:
                        raise e
                except Exception as e:
                    raise e

        return wrapper

    return decorator
