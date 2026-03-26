import pytest
from unittest.mock import MagicMock

from api.assistant.agent import SCORE_SORT_DIRECTION
from api.assistant.types import CatalogSearchResult, ContentSearchResult, Snippet
from api.blueprints.chat import prepare_search_response


def make_snippet(text, chunk_score):
    return Snippet(text=text, item_id=1, chunk_score=chunk_score)


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
        edition = ContentSearchResult(
            edition_id=1,
            chunk_hits=[],
            snippets=[make_snippet("a", 0.5)],
        )
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

        edition = CatalogSearchResult(
            edition_id=1,
            chunk_hits=[],
            agg_score=0.8,
            orm_work=MagicMock(),
            orm_edition=MagicMock(),
            snippets=[make_snippet("a", 0.5)],
        )
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
        edition = ContentSearchResult(edition_id=1, chunk_hits=[], snippets=snippets)
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
        edition = CatalogSearchResult(
            edition_id=1,
            chunk_hits=[],
            agg_score=0.7,
            orm_work=MagicMock(),
            orm_edition=MagicMock(),
            snippets=snippets,
        )
        search_results = make_search_results("search_catalog", [edition])

        _, formatted = prepare_search_response(search_results)

        output_scores = [s["chunk_score"] for s in formatted["editions"][0]["snippets"]]
        expected_scores = sorted(
            [s.chunk_score for s in snippets], **SCORE_SORT_DIRECTION
        )
        assert output_scores == expected_scores
