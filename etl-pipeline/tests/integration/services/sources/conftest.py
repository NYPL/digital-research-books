import sys
import threading
import time
from pathlib import Path

import pytest

RETRY_DELAY = 120  # seconds


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
    """Start a countdown timer when a test is retried."""
    if report.outcome == "retried":
        threading.Thread(
            target=_countdown, args=(report.nodeid, RETRY_DELAY), daemon=True
        ).start()


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
