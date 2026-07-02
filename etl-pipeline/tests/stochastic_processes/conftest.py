import pytest


@pytest.fixture(autouse=True)
def turbopuffer_namespace(monkeypatch):
    monkeypatch.setenv("TURBOPUFFER_NAMESPACE", "vra-10k-gemini_embedding_2")
