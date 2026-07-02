"""
Stochastic process tests for AI agent filter construction in catalog search.

These tests verify that the AI agent constructs appropriate filters based on
natural language queries. Since the agent's behavior involves an LLM, these
tests check expected patterns rather than exact deterministic outputs.

The search index calls are mocked to focus on testing filter construction.
"""

import json

import pytest
from pathlib import Path

from agents.items import ToolCallItem, ToolCallOutputItem

from api.assistant.agent import update_chat, META_OPERATORS, search_catalog
from api.assistant.models.filter import Filter
from tests.factories import stub_function_tool


_FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "stochastic_processes"


def _load_conversation_fixture(name: str) -> list:
    """Load a prior conversation history fixture (list of message dicts)."""
    return json.loads((_FIXTURES_DIR / name).read_text())


@pytest.fixture
def patch_search_catalog():
    """Fixture that stubs search_catalog to return 'No results found.'"""
    with stub_function_tool(search_catalog, "No results found."):
        yield


def get_last_tool_call_args(run_result) -> dict:
    """Return the deserialized arguments of the last tool call item in a run result."""
    tool_call_items = [
        item for item in run_result.new_items if isinstance(item, ToolCallItem)
    ]
    if not tool_call_items:
        raise ValueError(
            "No ToolCallItem found in run_result.new_items — no search tool calls were recorded"
        )
    return json.loads(tool_call_items[-1].raw_item.arguments)


# TODO: add dedicated unit tests for filter_match even though it is currently only used as a test helper
def filter_match(filters, attribute=None, operator=None, value=None) -> bool:
    """
    Recursively search a TurboPuffer-style filter tree for the first meta operator or leaf filter that
    matches all applicable given criteria.

    Args:
        filters: A Filter instance or raw filter data (list/tuple).
        attribute: List of acceptable attribute values, a callable ``(attr) -> bool``, or None to ignore.
        operator: List of acceptable operator values, a callable ``(op) -> bool``, or None to ignore.
            Note:  attribute and value must be None if operator will match 'And', 'Or', or 'Not'.
        value: List of acceptable values, a callable ``(val) -> bool``, or None to ignore.

    Returns:
        True as soon as a leaf filter or meta operator matches all non-None criteria, False otherwise.
    """
    if operator is not None and (
        any(operator(op) for op in META_OPERATORS)
        if callable(operator)
        else META_OPERATORS.intersection(operator)
    ):
        if attribute is not None or value is not None:
            raise ValueError(
                f"attribute and value must be None if operator includes any of {META_OPERATORS}"
            )

    if not isinstance(filters, Filter):
        filters = Filter.model_validate(filters)

    match filters.root:
        case ("And" | "Or", children):
            op = filters.root[0]
            if operator is not None and (
                operator(op) if callable(operator) else op in operator
            ):
                return True
            return any(
                filter_match(child, attribute=attribute, operator=operator, value=value)
                for child in children
            )
        case ("Not", child):
            if operator is not None and (
                operator("Not") if callable(operator) else "Not" in operator
            ):
                return True
            return filter_match(
                child, attribute=attribute, operator=operator, value=value
            )
        case (f_attr, f_op, f_val):
            if attribute is not None and not (
                attribute(f_attr) if callable(attribute) else f_attr in attribute
            ):
                return False
            if operator is not None and not (
                operator(f_op) if callable(operator) else f_op in operator
            ):
                return False
            if value is not None and not (
                value(f_val) if callable(value) else f_val in value
            ):
                return False
            return True


# TODO: parameterize over ContentSearch and CatalogSearch
class TestCatalogSearchFilterUsage:
    """Test that the agent constructs appropriate filters for catalog searches."""

    @pytest.mark.xfail(reason="Subject filter gets applied", raises=AssertionError)
    @pytest.mark.usefixtures("patch_search_catalog")
    def test_no_filter_for_generic_search(self, test_session):
        """
        Test: No filter is used when not needed (shipbuilding example).

        For a generic search like "shipbuilding", the agent should
        rely on semantic ranking without applying restrictive filters.
        """
        run_result = update_chat(
            "I want to learn about shipbuilding",
            conversation_type="catalogSearch",
            session=test_session,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # For a simple keyword search, filters should be None or minimal
        # The agent should rely on the ranking_query parameter
        # Either no filters applied, or only basic non-restrictive filters
        assert filters is None

    @pytest.mark.usefixtures("patch_search_catalog")
    def test_subject_filter(self, test_session):
        """
        Test: Filter is used when needed (poetry with mother-daughter themes).

        For a thematic search like "poetry that deals with mother daughter themes",
        the agent should apply subject filters to narrow results.
        """
        run_result = update_chat(
            "I want to find poetry that deals with mother daughter themes",
            conversation_type="catalogSearch",
            session=test_session,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None and filters != [], (
            "Expected filters in search tool args"
        )
        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: subject filter present
        assert filter_match(filters, attribute=["subject"]), (
            f"filters do not match expected criteria: {filters}"
        )

    @pytest.mark.usefixtures("patch_search_catalog")
    @pytest.mark.parametrize(
        "query",
        [
            pytest.param(
                "I want books about history but not military history",
                id="history_not_military",
            ),
            pytest.param(
                "I want books about astronomy but not astrology",
                id="astronomy_not_astrology",
            ),
        ],
    )
    def test_negative_filter(self, test_session, query):
        """
        Test: A negative filter is used when appropriate, and ranking_query uses positive framing only.

        For searches that exclude certain content, the agent should construct filters with
        negation operators. Negative language in ranking_query is washed out during semantic
        embedding and does not exclude content — exclusion must live entirely in a Not filter.
        """
        run_result = update_chat(
            query,
            conversation_type="catalogSearch",
            session=test_session,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None, "Expected filters in search tool args"
        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: Not operator present
        assert filter_match(filters, operator=["Not"]), (
            f"filters do not match expected criteria: {filters}"
        )

        # ranking_query should express positive intent; negation belongs in the Not filter
        ranking_query = search_params.get("ranking_query", "")
        ranking_tokens = set(ranking_query.lower().split())
        assert not (
            ranking_tokens
            & {"not", "excluding", "except", "without", "avoid", "exclude"}
        ), (
            f"ranking_query contains negative language that should be a filter: {ranking_query!r}"
        )

    @pytest.mark.usefixtures("patch_search_catalog")
    def test_multi_language_filter(self, test_session):
        """
        Test: Language filter construction uses ContainsAny for multiple languages.
        """
        run_result = update_chat(
            "I want books written English or French about philosophy",
            conversation_type="catalogSearch",
            session=test_session,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None, "Expected filters in search tool args"
        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: language filter with ContainsAny or two Contains conditions
        assert filter_match(
            filters,
            attribute=["language"],
            operator=["ContainsAny"],
            value=lambda v: set(v) == set(["French", "English"]),
        ) or (
            filter_match(
                filters, attribute=["language"], operator=["Contains"], value=["French"]
            )
            and filter_match(
                filters,
                attribute=["language"],
                operator=["Contains"],
                value=["English"],
            )
        ), f"filters do not match expected criteria: {filters}"

    @pytest.mark.usefixtures("patch_search_catalog")
    @pytest.mark.parametrize(
        "query,excluded_ranking_terms",
        [
            pytest.param(
                "Find books published between 2000 and 2010 about technology",
                frozenset(),
                id="date_range_explicit",
            ),
            pytest.param(
                "I want American poetry published before the Civil War",
                frozenset({"civil war"}),
                id="date_range_historical",
            ),
        ],
    )
    def test_date_range_filter(self, test_session, query, excluded_ranking_terms):
        """
        Test: Date range filters for publication dates.

        When searching for books published in a specific time period, the agent should
        construct appropriate date range filters. Temporal constraints should be expressed
        as publication_date filters on the metadata field, not embedded in ranking_query.
        """
        run_result = update_chat(
            query,
            conversation_type="catalogSearch",
            session=test_session,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None, "Expected filters in search tool args"
        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: publication_date filter with range operators
        assert filter_match(
            filters, attribute=["publication_date"], operator=["Gt", "Gte", "Lt", "Lte"]
        ), f"Expected date range filter with comparison operators, got: {filters}"

        # Temporal constraint phrases belong in the filter, not ranking_query
        if excluded_ranking_terms:
            ranking_query = search_params.get("ranking_query", "").lower()
            for phrase in excluded_ranking_terms:
                assert phrase not in ranking_query, (
                    f"Temporal phrase {phrase!r} should not appear in ranking_query "
                    f"(use a publication_date filter instead): {ranking_query!r}"
                )

    # def test_combined_filters_subject_and_language(self, mock_backend):
    #     """
    #     Test: Multiple filters combined with And operator.

    #     When search criteria include multiple constraints (e.g., subject and
    #     language), the agent should combine them appropriately.
    #     """
    #     conversation = [
    #         {
    #             "role": "user",
    #             "content": "I want French poetry books",
    #         }
    #     ]

    #     run_result = update_chat(conversation, conversation_type="catalogSearch")
    #     search_params = get_first_tool_args(run_result)
    #     filters = search_params.get("filters")

    #     # Should have combined filters
    #     if filters is not None and filters != []:
    #         filter_str = str(filters).lower()

    #         # Check for And operator when combining multiple criteria
    #         # (though the agent might structure this differently)
    #         assert (
    #             "and" in filter_str
    #             or ("subject" in filter_str and "language" in filter_str)
    #             or "french" in filter_str
    #         ), f"Expected combined filters for subject and language, got: {filters}"

    @pytest.mark.xfail(
        reason="author name is wrongly included in ranking_query", raises=AssertionError
    )
    @pytest.mark.usefixtures("patch_search_catalog")
    @pytest.mark.parametrize(
        "query,author_name",
        [
            pytest.param(
                "Find books written by Jane Austen",
                "austen",
                id="austen",
            ),
            pytest.param(
                "I want Walt Whitman's writing about democracy and the American spirit",
                "whitman",
                id="whitman",
            ),
        ],
    )
    def test_author_filter(self, test_session, query, author_name):
        """
        Test: Author filter for books by specific authors.

        When searching for books by a specific author, the agent should apply an author
        filter. Author attribution belongs in a structured author filter, not in
        ranking_query, which only performs semantic search over text content.
        """
        run_result = update_chat(
            query,
            conversation_type="catalogSearch",
            session=test_session,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None, "Expected filters in search tool args"
        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: author filter with ContainsAllTokens
        assert filter_match(
            filters,
            attribute=["author"],
            operator=["ContainsAllTokens"],
            value=lambda v: author_name in v.lower(),
        ), f"filters do not match expected criteria: {filters}"

        # Author name should not appear in ranking_query — it belongs in the author filter
        ranking_query = search_params.get("ranking_query", "")
        assert author_name not in ranking_query.lower(), (
            f"Author name should not appear in ranking_query (use author filter instead): {ranking_query!r}"
        )

    # MAYBE: remove this instruction and test because BM25 search achieves this \
    # functionality (better?) than a text field filter.
    @pytest.mark.usefixtures("patch_search_catalog")
    def test_text_filter(self, test_session):
        """
        Test: Exact phrase queries use ContainsTokenSequence on the text field.

        When the user asks for a specific named phrase, a ContainsTokenSequence
        filter on text ensures the phrase must appear verbatim in results rather
        than relying solely on semantic ranking, which cannot enforce exact spelling.
        """
        run_result = update_chat(
            "Find passages that mention the Magna Carta",
            conversation_type="catalogSearch",
            session=test_session,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None, "Expected filters in search tool args"
        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: ContainsTokenSequence on text field for the exact phrase
        assert filter_match(
            filters,
            attribute=["text"],
            operator=["ContainsTokenSequence"],
            value=lambda v: "magna carta" in v.lower(),
        ), f"Expected ContainsTokenSequence on text field for exact phrase: {filters}"

        assert search_params.get("ranking_query"), (
            "Expected a ranking_query alongside the phrase filter"
        )

    @pytest.mark.usefixtures("patch_search_catalog")
    def test_unsearchable_field_no_hallucinated_filter(self, test_session):
        """
        Test: Queries involving data not in the schema do not produce hallucinated filter fields.

        When the user requests content based on a property not in any indexed field (e.g.
        illustration metadata), the agent should execute a partial search using valid fields
        only, without inventing non-existent field names.
        """
        run_result = update_chat(
            "I want books that contain original maps or illustrations",
            conversation_type="catalogSearch",
            session=test_session,
            max_turns=1,
        )
        tool_call_items = [
            item for item in run_result.new_items if isinstance(item, ToolCallItem)
        ]
        assert len(tool_call_items) > 0, (
            "Expected a search to be attempted even for a partially unsearchable query"
        )

        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        if filters is not None:
            Filter.model_validate_json(filters)

    # MAYBE: the presence of any subject filter is inappropriate for this query. Change to not rely on subject filter (maybe just relay on bm25 in ranking filter)
    @pytest.mark.xfail(
        reason="ContainsAnyToken instead of ContainsAllTokens is used",
        raises=AssertionError,
    )
    @pytest.mark.usefixtures("patch_search_catalog")
    def test_compound_phrase(self, test_session):
        """
        Test: Compound phrases require all words.

        ContainsAnyToken splits the value string and matches if ANY token appears, so applying
        it to a compound phrase like "social contract" would match unrelated subjects containing
        only "social" or only "contract". ContainsAllTokens is more appropriate.
        """
        run_result = update_chat(
            "Find books on social contract theory",
            conversation_type="catalogSearch",
            session=test_session,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # No filter is acceptable — ranking_query handles it
        if filters is None:
            return

        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: compound phrase subject filter must not use ContainsAnyToken
        assert not filter_match(
            filters,
            attribute=["subject"],
            operator=["ContainsAnyToken"],
            value=lambda v: set(["social", "contract"]).issubset(v.lower().split()),
        ), "expected 'social' and 'contract' in subject filter."

    @pytest.mark.usefixtures("patch_search_catalog")
    def test_single_language_filter(self, test_session):
        """
        Test: A single-language filter uses 'Contains', not word token operators.

        The language field only supports Contains and ContainsAny. Token operators
        (ContainsAnyToken, ContainsAllTokens, ContainsTokenSequence) are not valid
        for the language field.
        """
        run_result = update_chat(
            "I want to find books written in German about philosophy",
            conversation_type="catalogSearch",
            session=test_session,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None, "Expected filters in search tool args"
        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: single language uses Contains
        assert filter_match(
            filters, attribute=["language"], operator=["Contains"], value=["German"]
        ), f"Expected ['language', 'Contains', 'German']: {filters}"

        # Assert content: language field must not use token operators
        assert not filter_match(
            filters,
            attribute=["language"],
            operator=["ContainsAnyToken", "ContainsAllTokens", "ContainsTokenSequence"],
        ), f"Language filter must not use token operators: {filters}"


# TODO: test authors with multiple spellings get multiple spellings in search/ranking_query e.g. avicenna

# TODO: test that in the unsearchable field case the agent response mentions that the desired filter criteria is not in available in metadata


@pytest.mark.parametrize(
    "query,prior_history",
    [
        # Target Error: Model writes ["And", cond1, cond2] instead of ["And", [cond1, cond2]] —
        # child conditions spread as variadic args rather than wrapped in an inner list.
        pytest.param(
            "Ornithology in the nineteenth century",
            None,
            id="and_or_wrong_nesting_1",
        ),
        pytest.param(
            "Theories of value and labor in 19th century economic thought",
            None,
            id="and_or_wrong_nesting_2",
        ),
        pytest.param(
            "The development of calculus and the Newton-Leibniz priority dispute",
            None,
            id="and_or_wrong_nesting_3",
        ),
        # Target Error: Model uses non-existent field names derived from UI facet labels
        # (e.g. publication_dateHeader, subjectSelection) instead of real schema fields.
        pytest.param(
            "European cartographic traditions before the Age of Exploration",
            None,
            id="hallucinated_field_mild",
        ),
        # Target Error: Model appends suffixes to both field names AND operators
        # (e.g. OrScroll, subjectScroll, ContainsAnyTokenScroll), producing
        # completely undeserializable filter JSON.
        pytest.param(
            "How did rapid urbanization affect social structures in 19th century Europe?",
            None,
            id="hallucinated_field_severe",
        ),
        # Target Error: Model substitutes integers or null in place of filter condition lists
        # (e.g. ["And", ["Or", 1, 2, 3, ...]]) rather than constructing real conditions.
        pytest.param(
            "19th century british poems",
            None,
            id="numeric_placeholder",
        ),
        # NOTE: this test case does not reproduce a search tool call... delete or replace with alternative json_string_conditions test case
        # # Target Error: Model correctly identifies the conditions it wants but serializes them as
        # # JSON strings instead of nested lists (e.g. ["And", ["[\"title\", \"Eq\", \"...\"]"]]).
        # pytest.param(
        #     "What is the source of the information of when the ottoman empire was found from?",
        #     _load_conversation_fixture("ottoman_prior_history_sessionId_e1d603c1-bc8d-4bf9-8c98-bb8133babbea.json"),
        #     id="json_string_conditions",
        # ),
        # Target Error: Model generates And/Or with an empty children list (e.g. ["Or", []]) or
        # passes a fully empty filter list.
        pytest.param(
            "Theories of planetary motion before Newton",
            None,
            id="empty_filter",
        ),
    ],
)
@pytest.mark.asyncio
async def test_filter_syntax_errors(test_session, query, prior_history):
    """
    Test: agent constructs TP filters with no syntax errors

    The agent should construct valid filters for each query without triggering
    a backend error.
    max_turns=1 keeps the agent to one attempt to construct filters without error.
    Each parametrized case targets an observed error category during testing
    (see inline comments on each pytest.param).
    """
    if prior_history is not None:
        # prior_history = _load_conversation_fixture() on export from agent_messages
        await test_session.add_items(prior_history)

    run_result = update_chat(
        query,
        conversation_type="catalogSearch",
        session=test_session,
        max_turns=1,
    )
    tool_call_items = [
        item for item in run_result.new_items if isinstance(item, ToolCallItem)
    ]
    tool_call_output_items = [
        item for item in run_result.new_items if isinstance(item, ToolCallOutputItem)
    ]

    assert len(tool_call_items) > 0, (
        "Expected at least one tool call, but none were made"
    )

    for item in tool_call_items:
        assert item.raw_item.name == "search_catalog", (
            f"Expected tool call to 'search_catalog', got '{item.raw_item.name}'"
        )
    for item in tool_call_output_items:
        assert not item.output.startswith("An error occurred while running the tool"), (
            f"Search tool call errored: {item.output}"
        )
