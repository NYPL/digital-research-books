import logging
import os
from typing import Iterator


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    log_level = os.environ.get("LOG_LEVEL", logging.INFO)
    logger.setLevel(log_level)
    return logger


def chunk(xs: Iterator, size: int) -> Iterator[list]:
    while True:
        chunk = []
        try:
            for _ in range(size):
                chunk.append(next(xs))

            yield chunk
        except StopIteration:
            if chunk:
                yield chunk

            break
