"""
Stochastic process tests for AI agent response quality.

These tests verify the content and style of agent responses using a mix of:
  - LLM-as-judge for semantic correctness
  - Structural assertions for deterministic properties (tool call presence)
"""

from pathlib import Path

import pytest

from agents.items import ToolCallItem

from api.assistant.agent import update_chat

from api.assistant.agent import search_catalog

from tests.factories import make_chunk_doc, stub_function_tool
from tests.stochastic_processes.llm_judge import llm_judge


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
        reason="The Judge's criteria should probably be loosened to accept the agent response in this case.",
    )
    @pytest.mark.asyncio
    async def test_grounding_fixture_inline(self, test_session, mock_search_backend):
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

        run_result = update_chat(
            "what is the plot of the lord of the rings",
            conversation_type="catalogSearch",
            session=test_session,
        )

        verdict = await llm_judge(
            run_result.to_input_list(),
            question=UNGROUNDED_INFORMATION_QUESTION,
        )

        assert verdict.answer == "NO", (
            f"Agent response contains ungrounded information.\nJudge reason: {verdict.reason}"
        )

    # TODO: parameterize the 2 grounding tests (bc they are identical besides \
    # the way the source the mocked search result)
    _GROUNDING_FIXTURE_PARAMS = [
        pytest.param(
            "what-is-the-delco-accent-search_catalog-result-2026-04-14.txt",
            "what is the delco accent",
            id="delco-accent",
        ),
        pytest.param(
            "tell-me-about-teeny-duchamp-search_catalog-result-2026-06-08.txt",
            "Tell me about Teeny Duchamp",
            id="teeny-duchamp",
        ),
    ]

    @pytest.mark.xfail
    @pytest.mark.parametrize("fixture_file,query", _GROUNDING_FIXTURE_PARAMS)
    @pytest.mark.asyncio
    async def test_grounding_fixture_file(self, test_session, fixture_file, query):
        """
        Verify that the agent response does not include information not grounded
        in the search results.

        Search results are stubbed from a fixture file saved from a real search
        tool response.
        """

        fixture_path = (
            Path(__file__).parents[1]
            / "fixtures"
            / "search_catalog_results"
            / fixture_file
        )

        with stub_function_tool(search_catalog, fixture_path.read_text()):
            run_result = update_chat(
                query,
                conversation_type="catalogSearch",
                session=test_session,
            )

        verdict = await llm_judge(
            run_result.to_input_list(),
            question=UNGROUNDED_INFORMATION_QUESTION,
        )

        assert verdict.answer == "NO", (
            f"Agent response contains ungrounded information.\nJudge reason: {verdict.reason}"
        )

    @pytest.mark.asyncio
    async def test_irrelevant_results_acknowledged(self, test_session):
        """
        Verify that the agent acknowledges search results are irrelevant to the query.
        All test cases should have no relevant documents in the search index.

        The search tool is stubbed to return a fixture containing results
        not related directly to the user query.
        """

        # TODO: consider using full session data fixture in
        # tests/fixtures/session_messages/ rather than tool call output only
        # fixture. The full session fixtures are more flexible for different
        # test types
        miyazaki_fixture = (
            Path(__file__).parents[1]
            / "fixtures"
            / "search_catalog_results"
            / "Hayao-Miyazaki-search_catalog-result-2026-04-14.txt"
        )

        with stub_function_tool(search_catalog, miyazaki_fixture.read_text()):
            run_result = update_chat(
                "Hayao Miyazaki",
                conversation_type="catalogSearch",
                session=test_session,
            )

        verdict = await llm_judge(
            run_result.to_input_list(),
            question="""\
Does the assistant response clearly acknowledge that the search
results do not include information directly related to the user query?
Answer YES if the response makes this clear, NO if it discusses \
the irrelevant results as if they are relevant to the query.""",
        )

        assert verdict.answer == "YES", (
            f"Agent did not acknowledge irrelevant results.\nJudge reason: {verdict.reason}"
        )

    def test_no_search_on_ambiguous_query(self, test_session):
        """
        Verify that the agent does not perform a search for an underspecified query.
        """

        query = "new york"
        with stub_function_tool(search_catalog, "No results found for your query."):
            run_result = update_chat(
                query,
                conversation_type="catalogSearch",
                session=test_session,
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
