"""
Integration tests and fixtures for get_relevant_snippets.

Provides two utilities for capturing and replaying RunResult state:
  - save_run_result_state(run_result, path)   — capture from a live run
  - load_mock_run_result(state)               — reconstruct a mock for replay

Usage (capturing):
    run_result = await update_chat(conversation, "catalogSearch")
    save_run_result_state(run_result, "tests/integration/api/assistant/agent/fixtures/run_result_state.json")

Usage (replaying):
    run_result = load_mock_run_result("tests/integration/api/assistant/agent/fixtures/run_result_state.json")
    run_result.last_agent.model._client = AsyncOpenAI(
        api_key=require_env("GOOGLE_API_KEY"),
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    )
    result = await get_relevant_snippets(run_result)
"""

import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from agents.items import ToolCallOutputItem
from openai import AsyncOpenAI

from api.assistant.agent import (
    ContentSearchResult,
    CatalogSearchResult,
    EditionSnippetLoop,
    Snippet,
    get_relevant_snippets,
)
from utils.common import require_env


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------


# ALT: just use format_search_results() to serialize, more replicable...
def serialize_run_result_state(run_result) -> dict:
    """Capture the run_result state needed to replay get_relevant_snippets."""
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

    tool_outputs = {}
    for item in run_result.new_items:
        if isinstance(item, ToolCallOutputItem):
            raw = item.raw_item
            call_id = (
                raw.get("call_id")
                if hasattr(raw, "get")
                else getattr(raw, "call_id", None)
            )
            if call_id is not None:
                tool_outputs[call_id] = str(item.output)

    return {
        "search_results": serialized_search_results,
        # _build_conversation_text does its own [:-1] slice
        "conversation": run_result.to_input_list(),
        "tool_outputs": tool_outputs,
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
    """Serialize run_result state to a JSON fixture file."""
    state = serialize_run_result_state(run_result)
    Path(path).write_text(json.dumps(state, indent=2, default=str))


def load_mock_run_result(
    state: dict | str | Path, client: AsyncOpenAI = None
) -> MagicMock:
    """Reconstruct a mock run_result for get_relevant_snippets from serialized state.

    Caller must set:
        run_result.last_agent.model._client = AsyncOpenAI(...)
    """
    if isinstance(state, (str, Path)):
        state = json.loads(Path(state).read_text())

    if client is None:
        client = AsyncOpenAI(
            api_key=require_env("GOOGLE_API_KEY"),
            base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        )

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

    new_items = []
    for call_id, output_text in state["tool_outputs"].items():
        item = MagicMock(spec=ToolCallOutputItem)
        item.raw_item = {"call_id": call_id}
        item.output = output_text
        new_items.append(item)

    run_result = MagicMock()
    run_result.context_wrapper.context.search_results = search_results
    run_result.context_wrapper.context.frbr_fields = state.get("frbr_fields")
    run_result.new_items = new_items
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

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "run_result_state.json"


@pytest.mark.asyncio
@pytest.mark.skipif(
    not FIXTURE_PATH.exists(),
    reason=f"Fixture not found at {FIXTURE_PATH}. Capture one with save_run_result_state().",
)
async def test_get_relevant_snippets_from_fixture():
    """Replay get_relevant_snippets against a captured run_result fixture."""
    run_result = load_mock_run_result(FIXTURE_PATH)
    run_result.last_agent.model._client = AsyncOpenAI(
        api_key=require_env("GOOGLE_API_KEY"),
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    )

    result = await get_relevant_snippets(run_result, fallback_naive=False)

    assert isinstance(result, list), f"get_relevant_snippets returned {result!r}"
    assert all(isinstance(loop, EditionSnippetLoop) for loop in result), (
        f"Unexpected items in result: {[r for r in result if not isinstance(r, EditionSnippetLoop)]}"
    )

    search_results = run_result.context_wrapper.context.search_results
    _, search_result = list(search_results.items())[-1]
    for entry in search_result["edition_data"]:
        assert entry.snippets, (
            f"Edition {entry.edition_id} has no AI-selected snippets after agent run"
        )
