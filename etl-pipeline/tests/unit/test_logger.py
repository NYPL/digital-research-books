import logging
import pytest

from logger import LogContextVars


class _RecordCapture(logging.Handler):
    """Handler that stores emitted LogRecord objects for inspection."""

    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)

    @property
    def last_message(self) -> str:
        return self.records[-1].getMessage()


@pytest.fixture()
def capture_logger():
    logger = logging.getLogger("test.log_context_vars")
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    handler = _RecordCapture()
    logger.addHandler(handler)
    yield logger, handler
    logger.handlers.clear()


class TestLogContextVars:
    def test_context_appended_to_message(self, capture_logger):
        logger, handler = capture_logger

        with LogContextVars(logger, context={"session_id": "abc"}):
            logger.info("test message")

        assert handler.last_message == "test message | session_id: abc"

    def test_context_not_present_outside_block(self, capture_logger):
        logger, handler = capture_logger

        with LogContextVars(logger, context={"session_id": "abc"}):
            pass

        logger.info("after context")
        assert handler.last_message == "after context"

    def test_empty_context_leaves_message_unchanged(self, capture_logger):
        logger, handler = capture_logger

        with LogContextVars(logger):
            logger.info("no context here")

        assert handler.last_message == "no context here"

    def test_multiline_message_appends_context_on_new_line(self, capture_logger):
        logger, handler = capture_logger

        with LogContextVars(logger, context={"request_id": "xyz"}):
            logger.info("line one\nline two")

        assert handler.last_message == "line one\nline two\nrequest_id: xyz"
