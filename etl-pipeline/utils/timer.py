import functools
import time


def timer(logger):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()

            logger.info(f"{func.__name__} took {end_time - start_time:.2f}s")

            return result

        return wrapper

    return decorator
