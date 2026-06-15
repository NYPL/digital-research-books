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
from tests.stochastic_processes.conftest import stub_function_tool


pytestmark = [pytest.mark.asyncio]

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


def filter_match(filters, attribute=None, operator=None, value=None):
    """
    Recursively search a TurboPuffer-style filter tree for the first meta operator or leaf filter that
    matches all applicable given criteria.

    Args:
        filters: A filter specification (list/tuple).
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

    if not isinstance(filters, (list, tuple)) or len(filters) == 0:
        return False

    op = filters[0]

    # Check meta operator match
    if operator is not None:
        if operator(op) if callable(operator) else op in operator:
            return True

    # Recurse meta filters
    if op in META_OPERATORS:
        if op == "Not":
            return filter_match(
                filters[1], attribute=attribute, operator=operator, value=value
            )
        else:
            # ["And"/"Or", [child_filter, ...]]
            return any(
                filter_match(child, attribute=attribute, operator=operator, value=value)
                for child in filters[1]
            )

    # Check simple leaf filter: [attribute, operator, value]
    try:
        f_attr, f_op, f_val = filters
    except (ValueError, TypeError) as e:
        raise ValueError(
            f"Expecting a simple filter [attribute, operator, value], got: {filters}"
        ) from e

    if attribute is not None and not (
        attribute(f_attr) if callable(attribute) else f_attr in attribute
    ):
        return False
    if operator is not None and not (
        operator(f_op) if callable(operator) else f_op in operator
    ):
        return False
    if value is not None and not (value(f_val) if callable(value) else f_val in value):
        return False

    return True


_VALID_SCHEMA_FIELDS = frozenset(
    {"text", "subject", "title", "author", "language", "publication_date"}
)
_NEGATIVE_WORDS = frozenset(
    {"not", "excluding", "except", "without", "avoid", "exclude"}
)


def collect_filter_fields(filters) -> set:
    """Walk a TurboPuffer filter tree and return all leaf attribute field names."""
    if not isinstance(filters, (list, tuple)) or len(filters) == 0:
        return set()
    op = filters[0]
    if op == "Not":
        return collect_filter_fields(filters[1])
    if op in META_OPERATORS:
        result = set()
        for child in filters[1]:
            result |= collect_filter_fields(child)
        return result
    try:
        f_attr, _, _ = filters
        return {f_attr}
    except (ValueError, TypeError):
        return set()


# TODO: mock search backend to just test filter construction


# TODO: parameterize over ContentSearch and CatalogSearch
class TestCatalogSearchFilterUsage:
    """Test that the agent constructs appropriate filters for catalog searches."""

    @pytest.mark.xfail(reason="Subject filter gets applied")
    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_no_filter_for_generic_search(self, test_session_id):
        """
        Test: No filter is used when not needed (shipbuilding example).

        For a generic search like "shipbuilding", the agent should
        rely on semantic ranking without applying restrictive filters.
        """
        run_result = await update_chat(
            "I want to learn about shipbuilding",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # For a simple keyword search, filters should be None or minimal
        # The agent should rely on the ranking_query parameter
        # Either no filters applied, or only basic non-restrictive filters
        assert filters is None

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_filters_used_for_metadata_search(self, test_session_id):
        """
        Test: Filter is used when needed (poetry with mother-daughter themes).

        For a thematic search like "poetry that deals with mother daughter themes",
        the agent should apply subject filters to narrow results.
        """
        run_result = await update_chat(
            "I want to find poetry that deals with mother daughter themes",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None and filters != [], (
            "Expected filters for subject filter for poetry search"
        )
        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: subject filter present
        assert filter_match(filters, attribute=["subject"]), (
            f"filters do not match expected criteria: {filters}"
        )

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_negative_filter_construction(self, test_session_id):
        """
        Test: A negative filter is used when appropriate.

        For searches that exclude certain content (e.g., "books about history
        but not military history"), the agent should construct filters with
        negation operators.
        """
        run_result = await update_chat(
            "I want books about history but not military history",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None, "Expected filters for exclusion search"
        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: Not operator present
        assert filter_match(filters, operator=["Not"]), (
            f"filters do not match expected criteria: {filters}"
        )

        # ranking_query should express positive intent; negation belongs in the Not filter
        ranking_query = search_params.get("ranking_query", "")
        ranking_tokens = set(ranking_query.lower().split())
        assert not (ranking_tokens & _NEGATIVE_WORDS), (
            f"ranking_query contains negative language that should be a filter: {ranking_query!r}"
        )

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_language_filter(self, test_session_id):
        """
        Test: Language filter construction uses ContainsAny for multiple languages.
        """
        run_result = await update_chat(
            "I want books written English or French about philosophy",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None, "Expected filters for language search"
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
    async def test_date_range_filter_construction(self, test_session_id):
        """
        Test: Date range filters for publication dates.

        When searching for books published in a specific time period,
        the agent should construct appropriate date range filters.
        """
        run_result = await update_chat(
            "Find books published between 2000 and 2010 about technology",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None, "Expected filters for date range search"

        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: publication_date filter with range operators
        assert filter_match(
            filters, attribute=["publication_date"], operator=["Gt", "Gte", "Lt", "Lte"]
        ), f"Expected date range filter with comparison operators, got: {filters}"

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

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_author_filter_construction(self, test_session_id):
        """
        Test: Author filter for books by specific authors.

        When searching for books by a specific author, the agent should
        apply author filters.
        """
        run_result = await update_chat(
            "Find books written by Jane Austen",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        # Assert filter exists
        assert filters is not None, "Expected filters for author search"
        # Assert filter valid
        filters = Filter.model_validate_json(filters).model_dump()

        # Assert content: author filter with ContainsAllTokens
        assert filter_match(
            filters,
            attribute=["author"],
            operator=["ContainsAllTokens"],
            value=lambda v: "austen" in v.lower(),
        ), f"filters do not match expected criteria: {filters}"

        # Author name should not appear in ranking_query — it belongs in the author filter
        ranking_query = search_params.get("ranking_query", "")
        assert "austen" not in ranking_query.lower(), (
            f"Author name should not appear in ranking_query (use author filter instead): {ranking_query!r}"
        )

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_exact_phrase_uses_token_sequence(self, test_session_id):
        """
        Test: Exact phrase queries use ContainsTokenSequence on the text field.

        When the user asks for a specific named phrase, a ContainsTokenSequence
        filter on text ensures the phrase must appear verbatim in results rather
        than relying solely on semantic ranking, which cannot enforce exact spelling.
        """
        run_result = await update_chat(
            "Find passages that mention the Magna Carta",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        assert filters is not None, "Expected a filter for exact phrase search"
        filters = Filter.model_validate_json(filters).model_dump()

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
    async def test_negative_filter_ranking_query_is_positive(self, test_session_id):
        """
        Test: When a Not filter is used for exclusion, ranking_query uses positive framing only.

        Negative language in ranking_query is washed out during semantic embedding and
        does not exclude content. Exclusion must live entirely in a Not filter.
        """
        run_result = await update_chat(
            "I want books about astronomy but not astrology",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        assert filters is not None, "Expected a filter for exclusion search"
        filters = Filter.model_validate_json(filters).model_dump()

        assert filter_match(filters, operator=["Not"]), (
            f"Expected a Not filter for the exclusion: {filters}"
        )

        ranking_query = search_params.get("ranking_query", "")
        ranking_tokens = set(ranking_query.lower().split())
        assert not (ranking_tokens & _NEGATIVE_WORDS), (
            f"ranking_query contains negative language that should be a filter: {ranking_query!r}"
        )

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_temporal_constraint_in_filter_not_ranking_query(
        self, test_session_id
    ):
        """
        Test: Temporal constraints go in publication_date filter, not in ranking_query.

        ranking_query only performs semantic search over text content. Publication period
        constraints should be expressed as publication_date filters on the metadata field.
        """
        run_result = await update_chat(
            "I want American poetry published before the Civil War",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        assert filters is not None, (
            "Expected a publication_date filter for temporal constraint"
        )
        filters = Filter.model_validate_json(filters).model_dump()

        assert filter_match(
            filters, attribute=["publication_date"], operator=["Lt", "Lte", "Gt", "Gte"]
        ), f"Expected a publication_date comparison filter: {filters}"

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_author_name_not_in_ranking_query(self, test_session_id):
        """
        Test: Author name goes in author filter, not ranking_query.

        ranking_query only performs semantic search over text content. Author attribution
        should be captured by an author filter on the structured metadata field.
        """
        run_result = await update_chat(
            "I want Walt Whitman's writing about democracy and the American spirit",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        assert filters is not None, "Expected an author filter"
        filters = Filter.model_validate_json(filters).model_dump()

        assert filter_match(
            filters,
            attribute=["author"],
            value=lambda v: isinstance(v, str) and "whitman" in v.lower(),
        ), f"Expected an author filter matching 'Whitman': {filters}"

        ranking_query = search_params.get("ranking_query", "")
        assert "whitman" not in ranking_query.lower(), (
            f"Author name should not appear in ranking_query (use author filter instead): {ranking_query!r}"
        )

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_unsearchable_field_no_hallucinated_filter(self, test_session_id):
        """
        Test: Queries involving data not in the schema do not produce hallucinated filter fields.

        When the user requests content based on a property not in any indexed field (e.g.
        illustration metadata), the agent should execute a partial search using valid fields
        only, without inventing non-existent field names.
        """
        run_result = await update_chat(
            "I want books that contain original maps or illustrations",
            conversation_type="catalogSearch",
            session_id=test_session_id,
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
            parsed = Filter.model_validate_json(filters).model_dump()
            used_fields = collect_filter_fields(parsed)
            assert used_fields <= _VALID_SCHEMA_FIELDS, (
                f"Filter references non-schema fields: {used_fields - _VALID_SCHEMA_FIELDS}"
            )

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_compound_phrase_not_any_token_alone(self, test_session_id):
        """
        Test: Multi-word subject terms use ContainsAllTokens/ContainsTokenSequence, not ContainsAnyToken.

        ContainsAnyToken splits the value string and matches if ANY token appears, so applying
        it to a compound phrase like "social contract" would match unrelated subjects containing
        only "social" or only "contract".
        """
        run_result = await update_chat(
            "Find books on social contract theory",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        if filters is None:
            return  # No filter is acceptable — ranking_query handles it

        filters = Filter.model_validate_json(filters).model_dump()

        assert not filter_match(
            filters,
            attribute=["subject"],
            operator=["ContainsAnyToken"],
            value=lambda v: isinstance(v, str)
            and "social" in v.lower().split()
            and "contract" in v.lower().split(),
        ), (
            f"Subject filter must not use ContainsAnyToken on compound phrase tokens "
            f"(use ContainsAllTokens or ContainsTokenSequence instead): {filters}"
        )

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_single_language_uses_contains(self, test_session_id):
        """
        Test: A single-language filter uses 'Contains', not word token operators.

        The language field only supports Contains and ContainsAny. Token operators
        (ContainsAnyToken, ContainsAllTokens, ContainsTokenSequence) are not valid
        for the language field.
        """
        run_result = await update_chat(
            "I want to find books written in German about philosophy",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        assert filters is not None, "Expected a language filter"
        filters = Filter.model_validate_json(filters).model_dump()

        assert filter_match(
            filters, attribute=["language"], operator=["Contains"], value=["German"]
        ), f"Expected ['language', 'Contains', 'German']: {filters}"

        assert not filter_match(
            filters,
            attribute=["language"],
            operator=["ContainsAnyToken", "ContainsAllTokens", "ContainsTokenSequence"],
        ), f"Language filter must not use token operators: {filters}"

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_language_value_properly_cased(self, test_session_id):
        """
        Test: Language filter values use ISO 639 full names with proper casing.

        Language values are case-sensitive in the search index. 'japanese' and 'JAPANESE'
        will not match; the correct form is 'Japanese'.
        """
        run_result = await update_chat(
            "show me books in japanese about art",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        assert filters is not None, "Expected a language filter"
        filters = Filter.model_validate_json(filters).model_dump()

        assert filter_match(
            filters,
            attribute=["language"],
            operator=["Contains", "ContainsAny"],
            value=lambda v: "Japanese" in v if isinstance(v, list) else v == "Japanese",
        ), f"Expected language value 'Japanese' (capital J, full ISO name): {filters}"

    @pytest.mark.usefixtures("patch_search_catalog")
    async def test_no_subject_filter_for_content_search(self, test_session_id):
        """
        Test: Pure content queries do not use subject filters.

        Subject filters are for genre/classification metadata (e.g. "poetry", "fiction").
        Content topic searches should rely on ranking_query so that relevant books are
        surfaced regardless of how they are catalogued under subject headings.
        """
        run_result = await update_chat(
            "I want to learn about the causes of the French Revolution",
            conversation_type="catalogSearch",
            session_id=test_session_id,
            max_turns=1,
        )
        search_params = get_last_tool_call_args(run_result)
        filters = search_params.get("filters")

        if filters is not None:
            parsed = Filter.model_validate_json(filters).model_dump()
            assert not filter_match(parsed, attribute=["subject"]), (
                f"Content topic query should not use subject filter "
                f"(use ranking_query instead): {filters}"
            )


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
async def test_filter_syntax_errors(test_session_id, query, prior_history):
    """
    Test: agent constructs TP filters with no syntax errors

    The agent should construct valid filters for each query without triggering
    a backend error.
    max_turns=1 keeps the agent to one attempt to construct filters without error.
    Each parametrized case targets an observed error category during testing
    (see inline comments on each pytest.param).
    """
    if prior_history is not None:
        from agents.extensions.memory.sqlalchemy_session import SQLAlchemySession
        from api.assistant.agent import get_async_engine

        session = SQLAlchemySession(test_session_id, engine=get_async_engine())
        # prior_history = _load_conversation_fixture() on extracted convo history
        await session.add_items(prior_history)

    run_result = await update_chat(
        query,
        conversation_type="catalogSearch",
        session_id=test_session_id,
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
