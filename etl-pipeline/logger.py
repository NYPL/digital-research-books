import logging
from newrelic.agent import NewRelicContextFormatter
import os
import sys


levels = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


def create_log(module):
    # set application root logger
    name = f"drb.{module}"
    return logging.getLogger(name)


def configure_loggers():
    """Read LOG_LEVEL and STAGE from environment, and configure 'drb' application
    root logger.
    """
    logger = logging.getLogger("drb")
    console_log_handler = logging.StreamHandler(stream=sys.stdout)

    log_level = os.environ.get("LOG_LEVEL", "info").lower()

    logger.setLevel(levels[log_level])
    console_log_handler.setLevel(levels[log_level])

    if "development" == os.environ.get("STAGE"):
        formatter = logging.Formatter("[%(name)s] %(message)s")
        console_log_handler.setFormatter(formatter)
    else:
        formatter = NewRelicContextFormatter(
            "%(asctime)s | %(name)s | %(levelname)s: %(message)s"
        )  # noqa: E501
        console_log_handler.setFormatter(formatter)

    logger.addHandler(console_log_handler)
