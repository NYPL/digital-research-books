import os

import pytest
from flask import Flask
from unittest.mock import AsyncMock, MagicMock

from api.assistant.agent import SCORE_SORT_DIRECTION
from api.assistant.types import CatalogSearchResult, ContentSearchResult, Snippet
from api.blueprints.chat import chat_blueprint, prepare_search_response


def make_snippet(text, chunk_score):
    return Snippet(text=text, item_id=1, chunk_score=chunk_score)


def make_edition_result(snippets, type="content"):
    if type == "content":
        return ContentSearchResult(
            edition_id=1, chunk_hits=[], snippets=snippets, frbr_fields={}
        )
    return CatalogSearchResult(
        edition_id=1,
        chunk_hits=[],
        agg_score=0.5,
        orm_work=MagicMock(),
        orm_edition=MagicMock(),
        snippets=snippets,
    )


def make_search_results(tool_name, edition_data, search_params=None):
    return {
        "tool_call_id_123": {
            "tool_name": tool_name,
            "search_params": search_params or {},
            "edition_data": edition_data,
        }
    }


class TestPrepareSearchResponse:
    def test_returns_none_none_when_no_results(self):
        result_type, result = prepare_search_response({})
        assert result_type is None
        assert result is None

    def test_content_search_output_structure(self):
        edition = make_edition_result([make_snippet("a", 0.5)])
        search_results = make_search_results("search_book", [edition])

        result_type, formatted = prepare_search_response(search_results)

        assert result_type == "contentSearch"
        assert set(formatted.keys()) == {"snippets", "search_params"}
        assert "editions" not in formatted
        assert "paging" not in formatted
        assert isinstance(formatted["snippets"], list)

    def test_catalog_search_output_structure(self, mocker):
        mocker.patch(
            "api.blueprints.chat.orm_to_dict", return_value={"some_field": "val"}
        )
        mocker.patch(
            "api.blueprints.chat.APIUtils.formatPagingOptions", return_value={"page": 1}
        )

        edition = make_edition_result([make_snippet("a", 0.5)], type="catalog")
        search_results = make_search_results("search_catalog", [edition])

        result_type, formatted = prepare_search_response(search_results)

        assert result_type == "catalogSearch"
        assert set(formatted.keys()) == {"editions", "search_params", "paging"}
        assert isinstance(formatted["editions"], list)
        assert "snippets" in formatted["editions"][0]

    def test_content_search_snippets_sorted(self):
        snippets = [
            make_snippet("a", 0.3),
            make_snippet("b", 0.9),
            make_snippet("c", 0.1),
        ]
        edition = make_edition_result(snippets)
        search_results = make_search_results("search_book", [edition])

        _, formatted = prepare_search_response(search_results)

        output_scores = [s["chunk_score"] for s in formatted["snippets"]]
        expected_scores = sorted(
            [s.chunk_score for s in snippets], **SCORE_SORT_DIRECTION
        )
        assert output_scores == expected_scores

    def test_catalog_search_snippets_sorted(self, mocker):
        mocker.patch("api.blueprints.chat.orm_to_dict", return_value={})
        mocker.patch(
            "api.blueprints.chat.APIUtils.formatPagingOptions", return_value={}
        )

        snippets = [
            make_snippet("a", 0.1),
            make_snippet("b", 0.7),
            make_snippet("c", 0.4),
        ]
        edition = make_edition_result(snippets, type="catalog")
        search_results = make_search_results("search_catalog", [edition])

        _, formatted = prepare_search_response(search_results)

        output_scores = [s["chunk_score"] for s in formatted["editions"][0]["snippets"]]
        expected_scores = sorted(
            [s.chunk_score for s in snippets], **SCORE_SORT_DIRECTION
        )
        assert output_scores == expected_scores


@pytest.fixture
def chat_test_client(mocker):
    """Flask test client for the /chat route with (a) low level external deps,
    (b) Runner.run(), and (c) Session / DB helpers mocked out.

    POST requests to the returned client must add header
    {"X-API-Key": "test-api-key"} to avoid errors raised by @require_api_key

    Yields (client, mock_runner_run).
    """
    mocker.patch.dict(
        os.environ,
        {
            "VRA_API_KEY": "test-api-key",  # matches "X-API-Key" header # pragma: allowlist secret
            "TURBOPUFFER_NAMESPACE": "test-namespace",
            "GOOGLE_API_KEY": "test-key",  # pragma: allowlist secret
        },
    )
    # require_session_jwt (JWT signing)
    mocker.patch("api.decorators.sign_session", return_value="fake-token")

    mocker.patch("api.assistant.agent.TurbopufferBackend")
    mocker.patch("api.assistant.agent.GoogleEmbedder")
    mocker.patch("api.assistant.agent.AsyncOpenAI")
    mocker.patch("api.assistant.agent.record_llm_events")

    # Runner.run returns a fake RunResult so all internal processes are mocked
    mock_run_result = MagicMock()
    mock_run_result.new_items = []
    mock_run_result.context_wrapper.context.search_results = {}
    mock_runner_run = mocker.patch(
        "api.assistant.agent.Runner.run",
        new_callable=AsyncMock,
        return_value=mock_run_result,
    )

    mocker.patch(
        "api.assistant.agent.get_frbr_data_by_edition",
        side_effect=NotImplementedError("patch get_frbr_data_by_edition in your test"),
    )
    mocker.patch(
        "api.assistant.agent.get_frbr_data_by_barcode",
        side_effect=NotImplementedError("patch get_frbr_data_by_barcode in your test"),
    )

    mocker.patch("api.blueprints.chat.get_async_engine")
    mocker.patch("api.blueprints.chat.SQLAlchemySession")
    mocker.patch("api.blueprints.chat.get_max_message_id", return_value=0)
    mocker.patch("api.blueprints.chat.get_session_messages_after", return_value=[])

    app = Flask("test")
    app.register_blueprint(chat_blueprint)
    with app.test_client() as client:
        yield client, mock_runner_run


def test_chat_passes_message_str_as_runner_input(chat_test_client):
    """Assert the raw message string from the request body reaches Runner.run(input=)."""
    client, mock_runner_run = chat_test_client
    message = "Find books about climate"

    client.post(
        "/chat",
        json={"message": message, "conversationType": "catalogSearch"},
        headers={"X-API-Key": "test-api-key"},
    )

    call_kwargs = mock_runner_run.call_args.kwargs
    assert call_kwargs["input"] == message
    assert isinstance(call_kwargs["input"], str)


def test_content_search_unknown_edition_returns_404(chat_test_client, mocker):
    """Assert that a contentSearch request with an unknown editionId returns 404."""
    FAKE_EDITION_ID = 99999

    # Empty return triggers the 404 response
    def fake_get_frbr_data_by_edition(edition_ids):
        assert edition_ids == [FAKE_EDITION_ID], (
            f"get_frbr_data_by_edition called with unexpected args: {edition_ids}"
        )
        return []

    mocker.patch(
        "api.assistant.agent.get_frbr_data_by_edition",
        side_effect=fake_get_frbr_data_by_edition,
    )

    client, _ = chat_test_client
    response = client.post(
        "/chat",
        json={
            "message": "Find something in this book",
            "conversationType": "contentSearch",
            "editionId": FAKE_EDITION_ID,
        },
        headers={"X-API-Key": "test-api-key"},
    )

    assert response.status_code == 404
    data = response.get_json()
    assert str(FAKE_EDITION_ID) in data["data"]["message"]
