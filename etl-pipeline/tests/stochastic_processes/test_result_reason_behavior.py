"""
Stochastic process tests for the /result-reason endpoint's explanation text.

Each test seeds a real session via a real update_chat() catalog search call
(same convention as test_agent_behavior.py / test_agent_responses.py), then
exercises the real result_reason Flask view through a test client -- so the
real DB-backed session lookup and real LLM call are used end-to-end, with
only auth bypassed (same pattern as tests/unit/api/blueprints/test_chat.py).
"""

import os

import pytest
from flask import Flask
from lxml import etree as ET

from agents.items import ToolCallItem, ToolCallOutputItem

from api.assistant.agent import search_catalog, update_chat
from api.blueprints.result_reason import result_reason_blueprint

from .conftest import stub_function_tool
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


@pytest.fixture
def result_reason_client(mocker):
    mocker.patch("newrelic.agent.add_custom_attribute")
    mocker.patch.dict(
        os.environ,
        {"VRA_API_KEY": "test-key"},  # pragma: allowlist secret
    )

    app = Flask("test")
    app.config["TESTING"] = True
    app.register_blueprint(result_reason_blueprint)
    client = app.test_client()
    client.set_cookie("vra_session", "test-token")
    return client


async def get_result_reason_explanation(
    run_result, barcode, result_reason_client, mocker, test_session_id
):
    """Seed via run_result (already produced by update_chat), then call the
    real /result-reason view and return the parsed response `data`."""
    call_id, found_barcode = find_call_id_and_barcode(run_result)
    barcode = barcode or found_barcode
    assert call_id is not None and barcode is not None, (
        f"No search tool call/barcode found in run_result.new_items: {run_result.new_items}"
    )

    mocker.patch("api.decorators.verify_session", return_value=test_session_id)

    response = result_reason_client.post(
        "/result-reason",
        json={"call_id": call_id, "barcode": barcode},
        headers={"X-API-Key": "test-key"},
    )
    assert response.status_code == 200, response.get_json()
    return response.get_json()["data"]


@pytest.fixture(scope="module")
def cached_catalog_query_result():
    """Cache catalog query run_results per query to avoid redundant LLM calls.

    NOTE: Tests must not mutate the returned run_result object, as it is
    shared across all tests that use this fixture.
    """
    cache = {}

    async def _run(query, test_session_id):
        if query not in cache:
            cache[query] = await update_chat(
                query, conversation_type="catalogSearch", session_id=test_session_id
            )
        return cache[query]

    return _run


async def test_result_reason_has_no_markdown_structure(
    cached_catalog_query_result, result_reason_client, mocker, test_session_id
):
    run_result = await cached_catalog_query_result(
        "fall of the Roman Empire", test_session_id
    )

    data = await get_result_reason_explanation(
        run_result, None, result_reason_client, mocker, test_session_id
    )

    assert_no_markdown_structure(data["explanation"])


async def test_irrelevant_result_reason_acknowledges_mismatch(
    result_reason_client, mocker, test_session_id
):
    """
    result_reason's own system prompt explicitly instructs it to tell the
    user when a result is only a closest-match, not truly relevant, and to
    hypothesize why it was returned. Verify that behavior via LLM judge when
    the underlying search result is clearly unrelated to the query.
    """
    with stub_function_tool(search_catalog, IRRELEVANT_RESULT_STUB):
        run_result = await update_chat(
            "Hayao Miyazaki",
            conversation_type="catalogSearch",
            session_id=test_session_id,
        )

    data = await get_result_reason_explanation(
        run_result,
        "33433012345678",
        result_reason_client,
        mocker,
        test_session_id,
    )

    verdict = await llm_judge(
        run_result,
        question=f"""\
A separate assistant call was asked to explain why the following book \
appeared as a search result for the query "Hayao Miyazaki":

Title: Index to records of the United States Strategic Bombing Survey

Its explanation was:
{data["explanation"]}

Does this explanation clearly acknowledge that the book is not truly \
relevant to the query and offer it only as a closest match, with a brief \
hypothesis for why it was returned? Answer YES if so, NO if it discusses \
the book as if it were a genuinely relevant result.""",
    )
    assert verdict.answer == "YES", (
        f"result_reason explanation did not acknowledge irrelevance.\n"
        f"Explanation: {data['explanation']}\n"
        f"Judge reason: {verdict.reason}"
    )
