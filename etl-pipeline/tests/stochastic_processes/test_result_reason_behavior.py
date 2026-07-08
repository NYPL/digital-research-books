"""
LLM behavioral tests for the /result-reason explanation-generation function.
"""

import json
from pathlib import Path

import pytest

from api.assistant.agent import find_result_by_barcode
from api.blueprints.result_reason import get_result_reason, get_tool_call_by_id

from tests.stochastic_processes.test_agent_behavior import assert_no_markdown_structure
from tests.stochastic_processes.llm_judge import llm_judge

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.xfail(reason="behavior unstable", strict=False),
]

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "session_messages"

ROMAN_EMPIRE_SESSION_ID = "928e4e37-1249-45b1-8002-6506a916e307"
MIYAZAKI_SESSION_ID = "c7699ffc-199e-4e2b-805c-41710d975cee"


def load_session_messages(session_id):
    """Load recorded agent_messages rows for a session from an ND-JSON fixture."""
    path = FIXTURES_DIR / f"{session_id}.jsonl"
    with path.open() as f:
        return [json.loads(line) for line in f if line.strip()]


def call_get_result_reason(messages, call_id, barcode):
    """Reproduce the /result-reason view's message-truncation and
    edition-lookup logic against recorded session messages, then call
    get_result_reason() directly.

    Returns: (explanation, is_ai_generated)
    """
    _, function_call_output, function_call_idx = get_tool_call_by_id(messages, call_id)
    assert function_call_output is not None, (
        f"call_id '{call_id}' not found in session messages"
    )

    edition_result = find_result_by_barcode(function_call_output, barcode)
    assert edition_result is not None, (
        f"barcode '{barcode}' not found in tool output for call_id '{call_id}'"
    )

    truncated_messages = messages[:function_call_idx]

    return get_result_reason(truncated_messages, edition_result)


async def test_result_reason_has_no_markdown_structure():
    messages = load_session_messages(ROMAN_EMPIRE_SESSION_ID)

    explanation, _ = call_get_result_reason(
        messages, call_id="hwz2505h", barcode="33433081565123"
    )

    assert_no_markdown_structure(explanation)


async def test_irrelevant_result_reason_acknowledges_mismatch():
    """
    result_reason's own system prompt explicitly instructs it to tell the
    user when a result is only a closest-match, not truly relevant, and to
    hypothesize why it was returned. Verify that behavior via LLM judge when
    the underlying search result is clearly unrelated to the query -- the
    Miyazaki fixture's search results include a WWII bombing-survey index
    that only matched on a "Lieutenant General Miyazaki" mentioned in its
    text.
    """
    messages = load_session_messages(MIYAZAKI_SESSION_ID)

    explanation, _ = call_get_result_reason(
        messages, call_id="azacg3fb", barcode="33433072847209"
    )

    verdict = await llm_judge(
        messages,
        question=f"""\
A separate assistant call was asked to explain why the following book \
appeared as a search result for the query "Hayao Miyazaki":

Title: Index to records of the United States Strategic Bombing Survey

Its explanation was:
{explanation}

Does this explanation clearly acknowledge that the book is not truly \
relevant to the query and offer it only as a closest match, with a brief \
hypothesis for why it was returned? Answer YES if so, NO if it discusses \
the book as if it were a genuinely relevant result.""",
    )
    assert verdict.answer == "YES", (
        f"result_reason explanation did not acknowledge irrelevance.\n"
        f"Explanation: {explanation}\n"
        f"Judge reason: {verdict.reason}"
    )
