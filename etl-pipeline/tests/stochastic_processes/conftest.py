import pytest
from pytest import StashKey, CollectReport
from sqlalchemy import text


phase_report_key = StashKey[dict[str, CollectReport]]()


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    """Store per-phase test outcome in item stash so fixtures can inspect it."""
    rep = yield
    item.stash.setdefault(phase_report_key, {})[rep.when] = rep
    return rep


TEST_SESSION_ID = "test"


@pytest.fixture
def test_session(request):
    """
    Fixture that provides a fixed session_id="test" for agent tests.

    Setup: deletes any stale data for the session_id.
    Teardown: on failure, prints the raw conversation before deleting;
              always deletes session data after the test.
    """
    from api.assistant.agent import delete_session_data
    from api.db import get_engine

    delete_session_data(TEST_SESSION_ID)

    yield TEST_SESSION_ID

    report = request.node.stash.get(phase_report_key, {})
    call_rep = report.get("call")
    if call_rep is not None and call_rep.failed:
        engine = get_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT * FROM agent_messages WHERE session_id = :sid ORDER BY id"
                ),
                {"sid": TEST_SESSION_ID},
            ).fetchall()
        print(f"\n--- Raw agent_messages for session '{TEST_SESSION_ID}' ---")
        for row in rows:
            print(dict(row._mapping))
        print("--- End of conversation ---\n")

    delete_session_data(TEST_SESSION_ID)
