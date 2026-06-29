import sys
import threading
import time
from pathlib import Path

import pytest
import requests

from services import SourceNotAvailableError

RETRY_DELAY = 120  # seconds

# Only retry then skip tests that fail due to these transient exceptions
RETRY_ON_EXCEPTIONS: list[type[BaseException]] = [
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    SourceNotAvailableError,
    requests.exceptions.HTTPError,
    # Add more exceptions as needed
]

# Tracks node IDs that have been marked "retried" by pytest-retry.
_retried_node_ids: set[str] = set()

# Maps node ID → (exception type, message) raised during the retry attempt.
_retry_exc_types: dict[str, tuple[type, str]] = {}


def pytest_set_filtered_exceptions() -> list[type[BaseException]]:
    """Retry tests only when they fail with certain exceptions."""
    return RETRY_ON_EXCEPTIONS


def _countdown(node_id: str, seconds: int, interval: int = 30) -> None:
    """Display a periodic countdown for realtime feedback when retrying a test."""
    test_name = node_id.split("/")[-1]
    sys.__stderr__.write(f"\n  RETRY {test_name}")
    sys.__stderr__.flush()
    for remaining in range(seconds, 0, -interval):
        sys.__stderr__.write(f"\n  Retrying {test_name} in {remaining:>3}s...\n")
        sys.__stderr__.flush()
        time.sleep(min(interval, remaining))
    sys.__stderr__.write("  Retrying now...\n")
    sys.__stderr__.flush()


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Record retried tests and start a countdown timer before the retry runs."""
    if report.outcome == "retried":
        # pytest-retry emits a "retried" report before each retry attempt.
        _retried_node_ids.add(report.nodeid)
        threading.Thread(
            target=_countdown, args=(report.nodeid, RETRY_DELAY), daemon=True
        ).start()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item: pytest.Item) -> None:
    """After retry attempt, capture the type if an Exception was raised."""
    outcome = yield
    if item.nodeid in _retried_node_ids and outcome.excinfo is not None:
        _retry_exc_types[item.nodeid] = (outcome.excinfo[0], str(outcome.excinfo[1]))


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo) -> None:
    """Mark tests as skipped if they fail with RETRY_ON_EXCEPTIONS error on second attempt.

    tryfirst=True ensures this runs after pytest-retry's tryfirst hookwrapper, as
    this is registered after it and therefore executed after it.

    Skip only when:
      - the final outcome is "failed" (retry did not pass)
      - the test was retried (node ID in _retried_node_ids)
      - the retry's exception is one of RETRY_ON_EXCEPTIONS
    """
    outcome = yield
    report = outcome.get_result()

    # retry_exc is non-None only if a retry has already occurred
    retry_exc = _retry_exc_types.get(report.nodeid)
    if (
        report.when == "call"
        and report.failed
        and retry_exc is not None
        and issubclass(retry_exc[0], tuple(RETRY_ON_EXCEPTIONS))
    ):
        report.outcome = "skipped"
        report.longrepr = (
            str(item.fspath),
            item.location[1],
            f"Skipped: retried once, second attempt failed with {retry_exc[0].__name__}: {retry_exc[1]}",
        )


def pytest_collection_modifyitems(items):
    """Add flaky marker to all tests under the sources folder to retry on failure."""
    for item in items:
        if Path(str(item.fspath)).is_relative_to(Path(__file__).parent):
            item.add_marker(pytest.mark.flaky(retries=1, delay=RETRY_DELAY))
            # Disable pytest-timeout: the global --timeout fires its SIGALRM
            # during the retry delay sleep, killing the xdist worker.
            # These tests usually fail on connection timeout so a test timeout
            # is not needed.
            item.add_marker(pytest.mark.timeout(0))
