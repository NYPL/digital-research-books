import pytest
from pathlib import Path


def pytest_collection_modifyitems(items):
    """Add flaky marker to all tests under the sources folder to retry on failure."""
    for item in items:
        if Path(str(item.fspath)).is_relative_to(Path(__file__).parent):
            item.add_marker(pytest.mark.flaky(retries=1, delay=120))
