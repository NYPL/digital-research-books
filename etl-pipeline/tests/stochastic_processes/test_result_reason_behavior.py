"""
LLM behavioral tests for the /result-reason explanation-generation function.

Each test seeds a real session via a real update_chat() catalog search call
(same convention as test_agent_behavior.py / test_agent_responses.py), then
calls get_result_reason() directly with the real session messages and
edition result -- so the real LLM call is exercised end-to-end. Call_id /
barcode validation and the view's 404 guards are out of scope here; this
file focuses solely on explanation content.
"""

import pytest
from lxml import etree as ET

from agents.items import ToolCallItem, ToolCallOutputItem

from api.assistant.agent import (
    find_result_by_barcode,
    get_session_messages,
    search_catalog,
    update_chat,
)
from api.blueprints.result_reason import get_result_reason, get_tool_call_by_id

from tests.factories import stub_function_tool
from tests.stochastic_processes.test_agent_behavior import assert_no_markdown_structure
from tests.stochastic_processes.test_agent_responses import llm_judge

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.xfail(reason="behavior unstable", strict=False),
]

# NOTE: none of the fixture files under tests/fixtures/search_catalog_results/
# include a <barcode> tag -- they predate display_book() adding barcode to
# catalog search output, so they can't be reused here since /result-reason
# requires a barcode. Using an inline stub instead.
IRRELEVANT_RESULT_STUB = """\
<search_results>
<edition>
<edition_id>15287442</edition_id>
<barcode>33433012345678</barcode>
<title>Index to records of the United States Strategic Bombing Survey</title>
<authors>United States Strategic Bombing Survey</authors>
<publisher>the United States Strategic Bombing Survey</publisher>
<date>1947-01-01</date>
<subjects>United States Strategic Bombing Survey -- Bibliography</subjects>
<language>English</language>
<chunks>
<chunk>
<item_id>24037108</item_id>
<page>312-313</page>
<text>
Serial No. 22. Subject: Shibawa Electric Company records from the
strategic bombing survey archives.
</text>
</chunk>
</chunks>
</edition>
</search_results>"""


def find_call_id_and_barcode(run_result):
    """Find the last search tool call's call_id and the first barcode in its output."""
    tool_call_items = [
        item for item in run_result.new_items if isinstance(item, ToolCallItem)
    ]
    if not tool_call_items:
        return None, None
    call_id = tool_call_items[-1].raw_item.call_id

    tool_output = next(
        (
            item.output
            for item in run_result.new_items
            if isinstance(item, ToolCallOutputItem)
            and item.raw_item["call_id"] == call_id
        ),
        None,
    )
    if not tool_output:
        return call_id, None

    try:
        root = ET.fromstring(tool_output)
        barcode_el = root.find(".//barcode")
    except ET.XMLSyntaxError:
        return call_id, None

    return call_id, (barcode_el.text if barcode_el is not None else None)


def call_get_result_reason(run_result, barcode, test_session_id):
    """Seed via run_result (already produced by update_chat), then reproduce
    the /result-reason view's message-truncation and edition-lookup logic
    and call get_result_reason() directly.

    Returns: (explanation, is_ai_generated)
    """
    call_id, found_barcode = find_call_id_and_barcode(run_result)
    barcode = barcode or found_barcode
    assert call_id is not None and barcode is not None, (
        f"No search tool call/barcode found in run_result.new_items: {run_result.new_items}"
    )

    messages = get_session_messages(test_session_id)
    _, function_call_output, function_call_idx = get_tool_call_by_id(messages, call_id)
    assert function_call_output is not None, (
        f"call_id '{call_id}' not found in session messages"
    )

    edition_result = find_result_by_barcode(function_call_output, barcode)
    assert edition_result is not None, (
        f"barcode '{barcode}' not found in tool output for call_id '{call_id}'"
    )

    messages = messages[:function_call_idx]

    return get_result_reason(messages, edition_result)


@pytest.fixture(scope="module")
def cached_catalog_query_result():
    """Cache catalog query run_results per query to avoid redundant LLM calls.

    NOTE: Tests must not mutate the returned run_result object, as it is
    shared across all tests that use this fixture.
    """
    cache = {}

    def _run(query, test_session):
        if query not in cache:
            cache[query] = update_chat(
                query, conversation_type="catalogSearch", session=test_session
            )
        return cache[query]

    return _run


async def test_result_reason_has_no_markdown_structure(
    cached_catalog_query_result,
    test_session,
    test_session_id,
):
    run_result = cached_catalog_query_result("fall of the Roman Empire", test_session)

    explanation, _ = call_get_result_reason(run_result, None, test_session_id)

    assert_no_markdown_structure(explanation)


async def test_irrelevant_result_reason_acknowledges_mismatch(
    test_session, test_session_id
):
    """
    result_reason's own system prompt explicitly instructs it to tell the
    user when a result is only a closest-match, not truly relevant, and to
    hypothesize why it was returned. Verify that behavior via LLM judge when
    the underlying search result is clearly unrelated to the query.
    """
    with stub_function_tool(search_catalog, IRRELEVANT_RESULT_STUB):
        run_result = update_chat(
            "Hayao Miyazaki",
            conversation_type="catalogSearch",
            session=test_session,
        )

    explanation, _ = call_get_result_reason(
        run_result, "33433012345678", test_session_id
    )

    verdict = await llm_judge(
        run_result,
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
