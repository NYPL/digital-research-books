"""
Stochastic process tests for AI agent filter construction in catalog search.

These tests verify that the AI agent constructs appropriate filters based on
natural language queries. Since the agent's behavior involves an LLM, these
tests check expected patterns rather than exact deterministic outputs.

The search index calls are mocked to focus on testing filter construction.
"""

import pytest
from pathlib import Path

from api.assistant.agent import update_chat, META_OPERATORS


pytestmark = pytest.mark.asyncio


def get_first_tool_args(run_result) -> dict:
    """Return the search_params dict from the first tool call in a run result."""
    return list(run_result.context_wrapper.context.search_results.values())[0][
        "search_params"
    ]


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


# MAYBE FUTURE: mock search backend to just test filter construction


class TestCatalogSearchFilterConstruction:
    """Test that the agent constructs appropriate filters for catalog searches."""

    async def test_no_filter_for_simple_keyword_search(self, test_session_id):
        """
        Test: No filter is used when not needed (shipbuilding example).

        For a simple keyword search like "shipbuilding", the agent should
        rely on semantic ranking without applying restrictive filters.
        """
        run_result = await update_chat(
            "I want to learn about shipbuilding",
            conversation_type="catalogSearch",
            session_id=test_session_id,
        )
        search_params = get_first_tool_args(run_result)
        filters = search_params.get("filters")

        # For a simple keyword search, filters should be None or minimal
        # The agent should rely on the ranking_query parameter
        # Either no filters applied, or only basic non-restrictive filters
        assert filters is None

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
        )
        search_params = get_first_tool_args(run_result)
        filters = search_params.get("filters")

        # Should have applied some filter
        assert filters is not None and filters != [], (
            "Expected filters for subject filter for poetry search"
        )

        assert filter_match(filters, attribute=["subject"]), (
            f"filters do not match expected criteria: {filters}"
        )

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
        )
        search_params = get_first_tool_args(run_result)
        filters = search_params.get("filters")

        # Should have applied some filter for exclusion
        assert filters is not None, "Expected filters for exclusion search"

        # At least one Not filter should be present
        assert filter_match(filters, operator=["Not"]), (
            f"filters do not match expected criteria: {filters}"
        )

    async def test_keyword_match_example(self, test_session_id):
        """
        Test: A keyword match filter for specific terminology.

        When searching for books with specific technical terms or exact
        phrases, the agent should use appropriate text matching filters.
        """
        run_result = await update_chat(
            'Find books that mention "machine learning" in their content',
            conversation_type="catalogSearch",
            session_id=test_session_id,
        )
        search_params = get_first_tool_args(run_result)

        filters = search_params.get("filters")

        # filter should require the phrase "machine learning"
        assert filter_match(
            filters,
            operator=["ContainsTokenSequence", "ContainsAllTokens"],
            value=lambda v: "machine learning" in v.lower(),
        ), f"filters do not match expected criteria: {filters}"

    async def test_language_filter(self, test_session_id):
        """
        Test: Language filter construction uses ContainsAny for multiple languages.
        """
        run_result = await update_chat(
            "I want books written English or French about philosophy",
            conversation_type="catalogSearch",
            session_id=test_session_id,
        )
        search_params = get_first_tool_args(run_result)
        filters = search_params.get("filters")

        # Should have language filter
        assert filters is not None, "Expected filters for language search"

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
        )
        search_params = get_first_tool_args(run_result)
        filters = search_params.get("filters")

        # Should have date filter
        assert filters is not None, "Expected filters for date range search"

        # Check for publication_date filter with range operators
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
        )
        search_params = get_first_tool_args(run_result)
        filters = search_params.get("filters")

        # Should have author filter
        assert filters is not None, "Expected filters for author search"

        assert filter_match(
            filters,
            attribute=["author"],
            operator=lambda o: "contains" in o.lower() and "token" in o.lower(),
            value=lambda v: "austen" in v.lower(),
        ), f"filters do not match expected criteria: {filters}"
