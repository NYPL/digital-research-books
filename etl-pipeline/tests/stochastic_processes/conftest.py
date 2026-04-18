import numpy as np
import pytest


# UNUSED - enhance in future
@pytest.fixture
def mock_search_backend(mocker):
    """
    Mock the search backend dependencies so tests focus on filter construction,
    not on live search results.

    - hybrid_search returns [] → search_catalog returns "No results found"
    - GoogleEmbedder.embed_one returns a dummy vector (called before hybrid_search)
    """
    mocker.patch("api.assistant.agent.hybrid_search", return_value=[])

    mock_embedder = mocker.MagicMock()
    mock_embedder.embed_one.return_value = np.zeros(768).tolist()
    mocker.patch("api.assistant.agent.GoogleEmbedder", return_value=mock_embedder)
