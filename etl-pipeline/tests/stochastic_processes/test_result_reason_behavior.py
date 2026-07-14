"""
LLM behavioral tests for the /result-reason explanation-generation function.
"""

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from api.assistant.agent import find_result_by_edition_id
from api.blueprints.result_reason import ResultReasonAgent, get_tool_call_by_id

from tests.stochastic_processes.test_agent_behavior import assert_no_markdown_structure
from tests.stochastic_processes.llm_judge import llm_judge


@dataclass(frozen=True)
class SampleResult:
    session_id: str
    call_id: str
    edition_id: int
    title: str


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "session_messages"

ROMAN_EMPIRE_SAMPLE_RESULT = SampleResult(
    session_id="928e4e37-1249-45b1-8002-6506a916e307",
    call_id="hwz2505h",
    edition_id=15546379,
    title="The history of the decline and fall of the Roman empire",
)

MIYAZAKI_SAMPLE_RESULT = SampleResult(
    session_id="c7699ffc-199e-4e2b-805c-41710d975cee",
    call_id="azacg3fb",
    edition_id=15287442,
    title="Index to records of the United States Strategic Bombing Survey",
)


def load_session_messages(session_id):
    """Load recorded agent_messages rows for a session from an ND-JSON fixture."""
    path = FIXTURES_DIR / f"{session_id}.jsonl"
    with path.open() as f:
        return [json.loads(line) for line in f if line.strip()]


def run_result_reason_agent(messages, call_id, edition_id):
    """Reproduce the /result-reason view's message-truncation and
    edition-lookup logic against recorded session messages, then run a
    ResultReasonAgent directly.

    Returns: (explanation, completions_messages)
    """
    _, function_call_output, function_call_idx = get_tool_call_by_id(messages, call_id)
    assert function_call_output is not None, (
        f"call_id '{call_id}' not found in session messages"
    )

    edition_result = find_result_by_edition_id(function_call_output, edition_id)
    assert edition_result is not None, (
        f"edition_id '{edition_id}' not found in tool output for call_id '{call_id}'"
    )

    truncated_messages = messages[:function_call_idx]

    agent = ResultReasonAgent(truncated_messages, edition_result)
    explanation, is_ai_generated = agent.run()
    assert is_ai_generated, "Failed to generate AI result reason"
    completions_messages = agent.completions_messages
    return explanation, completions_messages


def test_result_reason_has_no_markdown_structure():
    messages = load_session_messages(ROMAN_EMPIRE_SAMPLE_RESULT.session_id)

    explanation, _ = run_result_reason_agent(
        messages,
        call_id=ROMAN_EMPIRE_SAMPLE_RESULT.call_id,
        edition_id=ROMAN_EMPIRE_SAMPLE_RESULT.edition_id,
    )

    assert_no_markdown_structure(explanation)


def test_result_reason_does_not_restate_book_name():
    """
    result_reason's system prompt instructs it not to restate the book's
    name since the user already sees it elsewhere. Verify the exact title
    does not appear verbatim (case-insensitive) in the explanation.
    """
    messages = load_session_messages(ROMAN_EMPIRE_SAMPLE_RESULT.session_id)

    explanation, _ = run_result_reason_agent(
        messages,
        call_id=ROMAN_EMPIRE_SAMPLE_RESULT.call_id,
        edition_id=ROMAN_EMPIRE_SAMPLE_RESULT.edition_id,
    )

    assert ROMAN_EMPIRE_SAMPLE_RESULT.title.lower() not in explanation.lower(), (
        f"result_reason explanation restated the book title.\n"
        f"Explanation: {explanation}"
    )


@pytest.mark.asyncio
@pytest.mark.xfail(reason="behavior unstable", strict=False)
async def test_result_reason_does_not_state_relevance_level():
    """
    result_reason's system prompt instructs it to explain why a book is
    relevant without directly assessing a "relevance level" (e.g. "this is
    a highly relevant source" / "a close match"). Verify via LLM judge that
    the explanation sticks to substantive connection rather than a
    relevance-level characterization.
    """
    messages = load_session_messages(ROMAN_EMPIRE_SAMPLE_RESULT.session_id)

    explanation, completions_messages = run_result_reason_agent(
        messages,
        call_id=ROMAN_EMPIRE_SAMPLE_RESULT.call_id,
        edition_id=ROMAN_EMPIRE_SAMPLE_RESULT.edition_id,
    )

    verdict = await llm_judge(
        completions_messages,
        question="""\
Does the assistant's explanation directly characterize the book's *degree of relevance* anywhere \
(e.g. calling it "a highly relevant source", "a close match", "directly \
relevant", "tangentially related", etc.)? Answer YES if it \
makes any such relevance-level assessment, NO if it does not.""",
    )
    assert verdict.answer == "NO", (
        f"result_reason explanation stated a relevance level.\n"
        f"Explanation: {explanation}\n"
        f"Judge reason: {verdict.reason}"
    )


def test_result_reason_does_not_refer_to_search_system_as_actor():
    """
    result_reason's system prompt instructs it not to refer to "the search
    engine", "the search algorithm", "the system", etc. as an actor (e.g.
    "the search engine flagged xyz"). Verify none of those banned phrases
    appear in the explanation.
    """
    messages = load_session_messages(ROMAN_EMPIRE_SAMPLE_RESULT.session_id)

    explanation, _ = run_result_reason_agent(
        messages,
        call_id=ROMAN_EMPIRE_SAMPLE_RESULT.call_id,
        edition_id=ROMAN_EMPIRE_SAMPLE_RESULT.edition_id,
    )

    banned_phrases = [
        "search engine",
        "search algorithm",
        "search system",
        "the system",
    ]
    explanation_lower = explanation.lower()
    found = [phrase for phrase in banned_phrases if phrase in explanation_lower]
    assert not found, (
        f"result_reason explanation referred to the search system as an actor: {found}\n"
        f"Explanation: {explanation}"
    )


@pytest.mark.asyncio
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
    messages = load_session_messages(MIYAZAKI_SAMPLE_RESULT.session_id)

    explanation, completions_messages = run_result_reason_agent(
        messages,
        call_id=MIYAZAKI_SAMPLE_RESULT.call_id,
        edition_id=MIYAZAKI_SAMPLE_RESULT.edition_id,
    )

    verdict = await llm_judge(
        completions_messages,
        question="""\
Does the assistant explanation clearly acknowledge that the book is not truly \
relevant to the query and offer a brief \
hypothesis for why it was returned? Answer YES if so, NO if it discusses \
the book as if it were a genuinely relevant result.""",
    )
    assert verdict.answer == "YES", (
        f"result_reason explanation did not acknowledge irrelevance.\n"
        f"Explanation: {explanation}\n"
        f"Judge reason: {verdict.reason}"
    )
