import functools
import os
import psutil


def get_process_memory():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()

    return mem_info.rss / (1024**2)


def profile(logger):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            mem_before = get_process_memory()
            result = func(*args, **kwargs)
            mem_after = get_process_memory()

            logger.info(
                f"{func.__name__} memory usage (before {mem_before:.2f}: MB, after: {mem_after:.2f} MB, delta {(mem_after - mem_before):+.2f} MB)"
            )

            return result

        return wrapper

    return decorator
