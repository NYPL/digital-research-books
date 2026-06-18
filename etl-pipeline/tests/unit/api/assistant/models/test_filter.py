"""Unit tests for the TurboPuffer Filter pydantic model."""

import pytest
from pydantic import ValidationError

from api.assistant.models.filter import Filter


@pytest.mark.parametrize(
    "field",
    [
        "text",
        "subject",
        "title",
        "author",
        "language",
        "publication_date",
    ],
)
def test_all_allowed_fields(field):
    Filter.model_validate([field, "ContainsAnyToken", "foo"])


@pytest.mark.parametrize(
    "operator",
    [
        "Eq",
        "Contains",
        "ContainsAny",
        "Lt",
        "Lte",
        "Gt",
        "Gte",
        "ContainsAllTokens",
        "ContainsTokenSequence",
        "ContainsAnyToken",
    ],
)
def test_all_allowed_operators(operator):
    Filter.model_validate(["title", operator, "foo"])


@pytest.mark.parametrize(
    "data",
    [
        # --- Conditions: value types not covered by test_all_allowed_operators ---
        pytest.param(["subject", "Eq", None], id="condition_null_value"),
        pytest.param(
            ["language", "ContainsAny", ["Russian", "English"]],
            id="condition_list_value",
        ),
        # --- And ---
        pytest.param(
            [
                "And",
                [
                    ["author", "ContainsAnyToken", "Twain Hemingway"],
                    ["publication_date", "Gte", "1900-01-01"],
                ],
            ],
            id="and_basic",
        ),
        pytest.param(
            [
                "And",
                [
                    ["publication_date", "Gte", "1900-01-01"],
                    ["publication_date", "Lt", "2000-01-01"],
                    ["language", "Contains", "English"],
                ],
            ],
            id="and_multiple_children",
        ),
        # --- Or ---
        pytest.param(
            [
                "Or",
                [
                    ["language", "Contains", "French"],
                    ["language", "Contains", "Spanish"],
                ],
            ],
            id="or_basic",
        ),
        # --- Not ---
        pytest.param(
            ["Not", ["text", "ContainsAnyToken", "redacted censored"]],
            id="not_basic",
        ),
        # --- Nested ---
        pytest.param(
            [
                "And",
                [
                    ["publication_date", "Gte", "1800-01-01"],
                    [
                        "Or",
                        [
                            [
                                "subject",
                                "ContainsAnyToken",
                                "Science Chemistry Physics",
                            ],
                            ["author", "ContainsAllTokens", "Darwin"],
                        ],
                    ],
                ],
            ],
            id="nested_and_containing_or",
        ),
        pytest.param(
            [
                "And",
                [
                    ["publication_date", "Gte", "1900-01-01"],
                    ["publication_date", "Lt", "2000-01-01"],
                    ["Not", ["text", "ContainsAnyToken", "redacted censored"]],
                    [
                        "Or",
                        [
                            [
                                "subject",
                                "ContainsAnyToken",
                                "American literature English literature",
                            ],
                            [
                                "author",
                                "ContainsAnyToken",
                                "Fitzgerald Hemingway Faulkner Steinbeck",
                            ],
                        ],
                    ],
                ],
            ],
            id="nested_deeply_and_or_not",
        ),
    ],
)
def test_valid_filter(data):
    Filter.model_validate(data)


@pytest.mark.parametrize(
    "data",
    [
        # --- Invalid field names ---
        pytest.param(
            ["publication_dateHeader", "Gte", "1900-01-01"],
            id="hallucinated_field_with_suffix",
        ),
        pytest.param(
            ["subjectSelection", "ContainsAnyToken", "foo"],
            id="hallucinated_field_ui_label",
        ),
        # --- Invalid operators ---
        pytest.param(
            ["title", "NotEq", "foo"],
            id="excluded_operator",
        ),
        pytest.param(
            ["subject", "ContainsAnyTokenScroll", "foo"],
            id="hallucinated_operator_with_suffix",  # pragma: allowlist secret # lol this is over sensitive
        ),
        # --- Invalid value types ---
        pytest.param(
            ["publication_date", "Gte", 1900],
            id="integer_value",
        ),
        # --- Malformed And/Or structure ---
        pytest.param(
            [
                "And",
                ["publication_date", "Gte", "1900-01-01"],
                ["publication_date", "Lt", "2000-01-01"],
            ],
            id="and_conditions_not_wrapped_in_list",
        ),
        pytest.param(["And", []], id="and_empty_children"),
        pytest.param(["Or", []], id="or_empty_children"),
        pytest.param(["And", ["subject"]], id="and_single_string_child_not_wrapped"),
        # --- Placeholder / garbage values ---
        pytest.param(
            ["And", ["Or", 1, 2, 3]],
            id="numeric_placeholder_children",
        ),
        pytest.param([], id="empty_filter"),
        pytest.param([7, None], id="integer_field_and_null_operator"),
    ],
)
def test_invalid_filter(data):
    with pytest.raises(ValidationError):
        Filter.model_validate(data)
