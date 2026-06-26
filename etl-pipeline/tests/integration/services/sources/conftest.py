import pytest

pytestmark = pytest.mark.retry(retries=1, delay=120)

