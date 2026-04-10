import logging
from contextvars import ContextVar
from newrelic.agent import NewRelicContextFormatter
import os
import sys
from typing import Any


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


def get_app_logger() -> logging.Logger:
    return logging.getLogger("drb")


class _LogContextFilter(logging.Filter):
    """Format context vars with pipe (|) delimiter and append to log message"""

    def __init__(self, context_var: ContextVar):
        super().__init__()
        self._context_var = context_var

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = self._context_var.get()
        # NOTE: critical that this filter is a no-op for empty context_var bc in \
        # multi-threading context, multiple filters are added to the same \
        # logger and all executed, but only the filter from the local \
        # thread/co-routine would have data (bc of the isolation provided by \
        # ContextVars) and thus edit the record.msg
        if ctx:
            formatted = " | ".join(f"{k}: {v}" for k, v in ctx.items())
            if "\n" in record.getMessage():
                record.msg = str(record.msg) + f"\n{formatted}"
            else:
                record.msg = str(record.msg) + f" | {formatted}"
        return True


class LogContextVars:
    """
    Context manager that appends key/value context to logger messages
    for the duration of the block.
    ContextVars provide data isolation when used concurrently with
    multi-threading/asyncio.

    Usage::

        with LogContextVars(get_app_logger(), context={"session_id": abc}):
            logger.info("hello")  # -> "hello | session_id: abc"
    """

    def __init__(self, logger: logging.Logger, context: dict[str, Any] | None = None):
        self._logger = logger
        self._context = context or {}
        self._context_var: ContextVar[dict[str, Any]] = ContextVar(
            f"_log_context_{id(self)}", default={}
        )
        self._filter = _LogContextFilter(self._context_var)
        self._token = None

    def __enter__(self):
        self._token = self._context_var.set(self._context)
        for handler in self._logger.handlers:
            handler.addFilter(self._filter)
        return self

    def __exit__(self, *_):
        for handler in self._logger.handlers:
            handler.removeFilter(self._filter)
        self._context_var.reset(self._token)


def configure_loggers():
    """
    Configure 'drb' application root logger, using values from LOG_LEVEL and
    STAGE environment variables.
    """
    logger = get_app_logger()
    logger.handlers.clear()  # remove any pre-existing log handlers
    console_log_handler = logging.StreamHandler(stream=sys.stdout)

    print("log level", os.environ.get("LOG_LEVEL"))
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
