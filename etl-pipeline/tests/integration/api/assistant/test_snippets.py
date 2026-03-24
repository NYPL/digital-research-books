"""
Integration tests and fixtures for get_relevant_snippets.

Provides two utilities for capturing and replaying RunResult state:
  - save_run_result_state(run_result, path)   — capture from a live run
  - load_mock_run_result(state)               — reconstruct a mock for replay
"""

import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from openai import AsyncOpenAI

from api.assistant.snippets import (
    get_relevant_snippets_llm,
    get_relevant_snippets_naive,
    EditionSnippetLoop,
)
from api.assistant.types import (
    ContentSearchResult,
    CatalogSearchResult,
    Snippet,
)
from utils.common import require_env


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------


# ALT: just use prepare_search_response() to serialize, more replicable...
def serialize_run_result_state(run_result) -> dict:
    """Capture the run_result state needed execute get_relevant_snippets_llm."""
    context = run_result.context_wrapper.context
    search_results = context.search_results

    serialized_search_results = {}
    for call_id, sr in search_results.items():
        edition_data_out = []
        for entry in sr.get("edition_data", []):
            entry_data = {
                "edition_id": entry.edition_id,
                "chunk_hits": entry.chunk_hits,
                "snippets": [asdict(s) for s in entry.snippets],
            }
            if isinstance(entry, CatalogSearchResult):
                entry_data["agg_score"] = entry.agg_score
                entry_data["orm_work"] = {
                    "title": entry.orm_work.title,
                    "authors": entry.orm_work.authors,
                    "subjects": entry.orm_work.subjects,
                }
                entry_data["orm_edition"] = {
                    "publication_date": entry.orm_edition.publication_date,
                    "publishers": entry.orm_edition.publishers,
                    "languages": entry.orm_edition.languages,
                }
            edition_data_out.append(entry_data)
        serialized_search_results[call_id] = {
            "tool_name": sr.get("tool_name"),
            "search_params": sr.get("search_params"),
            "edition_data": edition_data_out,
        }

    return {
        "search_results": serialized_search_results,
        "conversation": run_result.to_input_list(),
        "agent": {
            "model_name": run_result.last_agent.model.model,
            "instructions": run_result.last_agent.instructions,
            "tool_name": run_result.last_agent.tools[0].name,
            "tool_description": run_result.last_agent.tools[0].description,
        },
        # frbr_fields present for contentSearch (ContentSearchExecutionContext)
        "frbr_fields": getattr(context, "frbr_fields", None),
    }


def save_run_result_state(run_result, path: str | Path) -> None:
    """Serialize run_result state to a JSON fixture file.

    Usage (capturing):
    run_result = await update_chat(conversation, "catalogSearch")
    save_run_result_state(run_result, "tests/integration/api/assistant/agent/fixtures/run_result_state.json")
    """
    state = serialize_run_result_state(run_result)
    Path(path).write_text(json.dumps(state, indent=2, default=str))


def load_mock_run_result(
    state: dict | str | Path, client: AsyncOpenAI = None
) -> MagicMock:
    """Reconstruct a mock run_result for get_relevant_snippets from serialized state.

    Usage (replaying):
        run_result = load_mock_run_result("tests/integration/api/assistant/agent/fixtures/run_result_state.json")
        result = await get_relevant_snippets_llm(run_result)
    """
    if isinstance(state, (str, Path)):
        state = json.loads(Path(state).read_text())

    if client is None:
        client = AsyncOpenAI(
            api_key=require_env("GOOGLE_API_KEY"),
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        )

    # build search result
    search_results = {}
    for call_id, sr in state["search_results"].items():
        edition_data = []
        for e in sr["edition_data"]:
            snippets = [Snippet(**s) for s in e["snippets"]]
            if "orm_work" in e and e["orm_work"] is not None:
                entry = CatalogSearchResult(
                    edition_id=e["edition_id"],
                    chunk_hits=e["chunk_hits"],
                    snippets=snippets,
                    agg_score=e["agg_score"],
                    orm_work=SimpleNamespace(**e["orm_work"]),
                    orm_edition=SimpleNamespace(**e["orm_edition"]),
                )
            else:
                entry = ContentSearchResult(
                    edition_id=e["edition_id"],
                    chunk_hits=e["chunk_hits"],
                    snippets=snippets,
                )
            edition_data.append(entry)
        search_results[call_id] = {
            "tool_name": sr["tool_name"],
            "search_params": sr["search_params"],
            "edition_data": edition_data,
        }

    # Assemble RunResult mock
    run_result = MagicMock()
    run_result.context_wrapper.context.search_results = search_results
    run_result.context_wrapper.context.frbr_fields = state.get("frbr_fields")
    run_result.to_input_list.return_value = state["conversation"]
    run_result.last_agent.model.model = state["agent"]["model_name"]
    run_result.last_agent.instructions = state["agent"]["instructions"]
    run_result.last_agent.tools[0].name = state["agent"]["tool_name"]
    run_result.last_agent.tools[0].description = state["agent"]["tool_description"]
    run_result.last_agent.model._client = client

    return run_result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

FIXTURE_PATH = Path(__file__).parents[3] / "fixtures" / "run_result_state.json"


def test_get_relevant_snippets_naive():
    if not FIXTURE_PATH.exists():
        raise FileNotFoundError(
            f"Fixture not found at {FIXTURE_PATH}. Capture one with save_run_result_state()."
        )
    """Check that naive snippets are successfully populated for each edition."""

    run_result = load_mock_run_result(FIXTURE_PATH, client=MagicMock())

    # Assert fixture starts with no snippets
    search_results = run_result.context_wrapper.context.search_results
    _, search_result = list(search_results.items())[-1]
    for entry in search_result["edition_data"]:
        assert entry.snippets == [], (
            f"Edition {entry.edition_id} already has snippets before agent run"
        )

    result = get_relevant_snippets_naive(run_result)

    # assert return value
    assert result is True, f"get_relevant_snippets_naive returned {result!r}"

    # Assert at least 1 naive snippet was saved for each edition
    # TODO: assert num snippets == num chunks per edition entry
    for entry in search_result["edition_data"]:
        assert entry.snippets, (
            f"Edition {entry.edition_id} has no naive snippets after run"
        )


@pytest.mark.asyncio
async def test_get_relevant_snippets_llm():
    if not FIXTURE_PATH.exists():
        raise FileNotFoundError(
            f"Fixture not found at {FIXTURE_PATH}. Capture one with save_run_result_state()."
        )
    """Check that snippets are successfully selected."""

    run_result = load_mock_run_result(FIXTURE_PATH)

    # Assert fixture starts with no snippets
    search_results = run_result.context_wrapper.context.search_results
    _, search_result = list(search_results.items())[-1]
    for entry in search_result["edition_data"]:
        assert entry.snippets == [], (
            f"Edition {entry.edition_id} already has snippets before agent run"
        )

    result = await get_relevant_snippets_llm(run_result, fallback_naive=False)

    # assert return type
    assert isinstance(result, list), f"get_relevant_snippets returned {result!r}"
    assert all(isinstance(loop, EditionSnippetLoop) for loop in result), (
        f"Unexpected items in result: {[r for r in result if not isinstance(r, EditionSnippetLoop)]}"
    )

    # Assert at least 1 LLM selected snippet was saved for each edition
    for entry in search_result["edition_data"]:
        assert entry.snippets, (
            f"Edition {entry.edition_id} has no AI-selected snippets after agent run"
        )
