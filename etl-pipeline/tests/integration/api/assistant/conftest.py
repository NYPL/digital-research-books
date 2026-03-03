import os
from unittest.mock import patch

import pytest
from scripts.create_user import create_user
from sqlalchemy.exc import IntegrityError
from utils.common import require_env
from logger import create_log

logger = create_log(__name__)

TEST_USERNAME = "vra_integration_test_user"


@pytest.fixture(scope="session")
def vra_test_user(setup_env):
    """
    Ensures a shared test user exists in the database and patches VRA_TEST_USERNAME
    and VRA_TEST_PASSWORD into the environment for authentication testing.

    Uses the same hashing approach as scripts/create_user.py.
    If the user already exists in the DB, logs and moves on — the password
    is read from VRA_TEST_USER_PASSWORD in the environment (e.g. .env.local),
    so credentials are never committed to the repository.
    """
    test_password = require_env("VRA_TEST_PASSWORD")
    try:
        create_user(TEST_USERNAME, test_password)
    except IntegrityError:
        logger.info(f"Test user '{TEST_USERNAME}' already exists, skipping creation.")

    with patch.dict(
        os.environ,
        {"VRA_TEST_USERNAME": TEST_USERNAME, "VRA_TEST_PASSWORD": test_password},
    ):
        yield {"username": TEST_USERNAME, "password": test_password}

    # TODO: Tear down - delete the test user from the database
