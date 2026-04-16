"""
Stochastic process tests for AI agent response quality.

These tests verify the content and style of agent responses using a mix of:
  - LLM-as-judge for semantic correctness
  - Structural assertions for deterministic properties (tool call presence, markdown)
"""

import json
import re
from pathlib import Path
from typing import Literal

import pytest
from openai import AsyncOpenAI
from pydantic import BaseModel

from agents.items import ToolCallItem

from api.assistant.agent import update_chat
from utils.common import require_env

from .conftest import make_chunk_doc, stub_search_catalog


pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# LLM-as-judge
# ---------------------------------------------------------------------------


class JudgeVerdict(BaseModel):
    reason: str
    answer: Literal["YES", "NO"]


# NOTE: async bc called inside test which is async bc it tests update_chat()
async def llm_judge(run_result, question: str) -> JudgeVerdict:
    """
    Run an LLM-as-judge evaluation over the full conversation history.

    The judge receives the serialized conversation (run_result.to_input_list())
    embedded in the system prompt, then answers a YES/NO question about it.
    `default=str` handles non-serializable items in the history (e.g. datetime).
    """
    client = AsyncOpenAI(
        api_key=require_env("GOOGLE_API_KEY"),
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    )
    conversation_json = json.dumps(run_result.to_input_list(), indent=2, default=str)

    response = await client.chat.completions.parse(
        model="gemini-3.1-pro-preview",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an impartial evaluator judging an AI assistant's "
                    "conversation. You are given the full conversation history below, "
                    "including tool call inputs and outputs. Answer the evaluation "
                    "question with YES or NO and explain your reasoning.\n\n"
                    f"CONVERSATION:\n{conversation_json}"
                ),
            },
            {
                "role": "user",
                "content": f"EVALUATION QUESTION: {question}",
            },
        ],
        response_format=JudgeVerdict,
        temperature=0,
    )
    parsed = response.choices[0].message.parsed
    assert parsed is not None, "LLM judge returned a null response (possible refusal)"

    print("QUESTION:", question)
    print("AGENT FINAL RESPONSE:", run_result.final_output)
    print("VERDICT:", parsed.answer)
    print("REASON:", parsed.reason)
    return parsed


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

UNGROUNDED_INFORMATION_QUESTION = """\
Does the assistant response include any specific, verifiable statements of fact \
that are NOT present in the search tool output shown in the conversation or \
the system instructions shown below?

Facts from the assistant's system instructions:
* The catalog the agent searches is public domain books only.
* The search tool uses embedding similarity search.

Answer YES if such ungrounded information is present, NO if the \
response stays within what the search results and the system instructions contain."""

# MAYBE: mock agents.SQLAlchemySession instead of using test_session_id fixture


class TestAgentResponses:
    # TODO: make synthetic results closer to user query topic
    # MAYBE: save the synthetic search tool output as a fixture instead?
    @pytest.mark.xfail(
        reason="The Judge's criteria should probably be loosened to accept the agent response in this case."
    )
    async def test_grounding(self, test_session_id, mock_search_backend):
        """
        Verify that the agent response does not include information not grounded
        in the search results.

        Search results are mocked inside the test with synthetic ChunkDocuments.
        """

        # Build synthetic search results
        chunk_docs = [
            make_chunk_doc(
                title="Engineering Marvels of the Roman Empire",
                text=(
                    "Roman aqueducts were one of the greatest engineering achievements "
                    "of the ancient world. They transported water over long distances "
                    "using gravity-fed channels and arched bridges. The Aqua Claudia, "
                    "built between 38 and 52 AD, stretched nearly 69 kilometers."
                ),
                edition_id=101,
                book_id="1001",
                chunk_index=0,
                subject=["Civil engineering", "Roman history"],
                author=["Marcus Vitruvius"],
            ),
            make_chunk_doc(
                title="Engineering Marvels of the Roman Empire",
                text=(
                    "The construction of aqueducts required precise surveying. "
                    "A gradient of roughly 1 in 4800 was maintained across the entire "
                    "length to ensure steady flow without erosion. Workers used the "
                    "chorobates, a leveling instrument, for measurement."
                ),
                edition_id=101,
                book_id="1001",
                chunk_index=1,
                subject=["Civil engineering", "Roman history"],
                author=["Marcus Vitruvius"],
            ),
            make_chunk_doc(
                title="The Art of French Cookery in the Eighteenth Century",
                text=(
                    "French haute cuisine in the 1700s was defined by rich sauces and "
                    "elaborate presentation. The roux, a cooked paste of butter and "
                    "flour, formed the basis of most classical sauces including "
                    "béchamel and velouté."
                ),
                edition_id=102,
                book_id="1002",
                chunk_index=0,
                subject=["Cooking", "French history"],
                author=["François Menon"],
            ),
            make_chunk_doc(
                title="The Art of French Cookery in the Eighteenth Century",
                text=(
                    "A consommé requires long simmering of bones and vegetables to "
                    "achieve clarity and depth. The addition of egg whites as a raft "
                    "removes impurities and allows the broth to become perfectly "
                    "transparent."
                ),
                edition_id=102,
                book_id="1002",
                chunk_index=1,
                subject=["Cooking", "French history"],
                author=["François Menon"],
            ),
        ]
        mock_search_backend(chunk_docs)

        run_result = await update_chat(
            "what is the plot of the lord of the rings",
            conversation_type="catalogSearch",
            session_id=test_session_id,
        )

        verdict = await llm_judge(
            run_result,
            question=UNGROUNDED_INFORMATION_QUESTION,
        )

        assert verdict.answer == "NO", (
            f"Agent response contains ungrounded information.\nJudge reason: {verdict.reason}"
        )

    # TODO: parameterize the 2 grounding tests (bc they are identical besides \
    # the way the source the mocked search result)
    @pytest.mark.xfail
    async def test_grounding_delco_accent(self, test_session_id):
        """
        Verify that the agent response does not include information not grounded
        in the search results, for a query about the Delco accent.

        Search results are stubbed from a fixture file.
        """

        delco_fixture = (
            Path(__file__).parents[1]
            / "fixtures"
            / "what-is-the-delco-accent-search_catalog-result-2026-04-14.txt"
        )

        with stub_search_catalog(delco_fixture.read_text()):
            run_result = await update_chat(
                "what is the delco accent",
                conversation_type="catalogSearch",
                session_id=test_session_id,
            )

        verdict = await llm_judge(
            run_result,
            question=UNGROUNDED_INFORMATION_QUESTION,
        )

        assert verdict.answer == "NO", (
            f"Agent response contains ungrounded information.\nJudge reason: {verdict.reason}"
        )

    async def test_irrelevant_results_acknowledged(self, test_session_id):
        """
        Verify that the agent acknowledges search results are irrelevant to the query.

        The search tool is stubbed to return a fixture containing results
        not related directly to the user query.
        """

        miyazaki_fixture = (
            Path(__file__).parents[1]
            / "fixtures"
            / "Hayao-Miyazaki-search_catalog-result-2026-04-14.txt"
        )

        with stub_search_catalog(miyazaki_fixture.read_text()):
            run_result = await update_chat(
                "Hayao Miyazaki",
                conversation_type="catalogSearch",
                session_id=test_session_id,
            )

        verdict = await llm_judge(
            run_result,
            question="""\
Does the assistant response clearly acknowledge that the search
results do not include information directly related to the user query?
Answer YES if the response makes this clear, NO if it discusses \
the irrelevant results as if they are relevant to the query.""",
        )

        assert verdict.answer == "YES", (
            f"Agent did not acknowledge irrelevant results.\nJudge reason: {verdict.reason}"
        )

    @pytest.mark.xfail
    async def test_no_search_on_ambiguous_query(self, test_session_id):
        """
        Verify that the agent does not perform a search for an underspecified query.
        """

        query = "new york"
        with stub_search_catalog("No results found for your query."):
            run_result = await update_chat(
                query,
                conversation_type="catalogSearch",
                session_id=test_session_id,
            )

        tool_calls = [
            item for item in run_result.new_items if isinstance(item, ToolCallItem)
        ]

        # TODO: add llm_judge check that a clarifying follow-up question is actually
        #       present in the agent's final response text.
        assert len(tool_calls) == 0, (
            f"Expected no search tool calls for ambiguous query '{query}', "
            f"but found {len(tool_calls)} call(s)."
        )

    async def test_no_markdown_in_response(self, test_session_id):
        """
        Verify that the agent response does not contain markdown formatting.
        This checks the final output for common markdown elements using regex patterns.
        """

        with stub_search_catalog("No results found for your query."):
            run_result = await update_chat(
                "north american early medicine",
                conversation_type="catalogSearch",
                session_id=test_session_id,
            )

        response_text = run_result.final_output

        markdown_patterns = {
            "heading": r"^#{1,6}\s",
            "bold": r"\*\*[^*]+\*\*|__[^_]+__",
            "inline_code": r"`[^`]+`",
            "code_block": r"```",
            "link": r"\[.+?\]\(.+?\)",
        }

        violations = [
            name
            for name, pattern in markdown_patterns.items()
            if re.search(pattern, response_text, re.MULTILINE)
        ]

        assert not violations, (
            f"Agent response contains markdown formatting: {violations}\n"
            f"Response:\n{response_text}"
        )
