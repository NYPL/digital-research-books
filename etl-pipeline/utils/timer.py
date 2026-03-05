import functools
from time import perf_counter


def timer(logger):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                logger.info(f"{func.__name__} took {perf_counter() - start_time:.2f}s")

        return wrapper

    return decorator
