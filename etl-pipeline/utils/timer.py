import asyncio
import functools
from time import perf_counter


def timer(logger):
    """Decorator that logs execution time of functions.

    Handles sync or async functions by detecting whether the wrapped function is
    a coroutine function and returning the appropriate wrapper.
    """

    def decorator(func):
        fmt = "{name}() took {elapsed:.2f}s"

        # async
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                start_time = perf_counter()
                try:
                    return await func(*args, **kwargs)
                finally:
                    logger.info(
                        fmt.format(
                            name=func.__qualname__, elapsed=perf_counter() - start_time
                        )
                    )
        # sync
        else:

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = perf_counter()
                try:
                    return func(*args, **kwargs)
                finally:
                    logger.info(
                        fmt.format(
                            name=func.__qualname__, elapsed=perf_counter() - start_time
                        )
                    )

        return wrapper

    return decorator
