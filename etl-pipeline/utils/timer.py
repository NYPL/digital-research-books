import functools
from time import perf_counter


def timer(logger):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = perf_counter()
            result = func(*args, **kwargs)
            end_time = perf_counter()

            logger.info(f"{func.__name__} took {end_time - start_time:.2f}s")

            return result

        return wrapper

    return decorator
