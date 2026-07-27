def pytest_collection_modifyitems(items):
    """Default xfail markers in this directory to raises=AssertionError.

    Stochastic (LLM-behavioral) tests use xfail to tolerate known-unstable
    LLM judgments, but should never silently swallow unrelated bugs (typos,
    missing fixtures, etc). Scoping to raises=AssertionError here means new
    xfail markers get this behavior without each author remembering to set
    raises= by hand.
    """
    for item in items:
        marker = item.get_closest_marker("xfail")
        if marker is not None and "raises" not in marker.kwargs:
            marker.kwargs["raises"] = AssertionError
