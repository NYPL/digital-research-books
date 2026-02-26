"""
Unit tests for agent filter post-processing functions.

Tests the filter transformation pipeline including datetime conversion
and null-matching for incomplete attributes.
"""

import pytest
from datetime import datetime
from api.assistant.agent import (
    convert_datetime_value,
    add_null_match,
    process_filters_recursively,
    apply_filter_transforms,
    INCOMPLETE_ATTRIBUTES,
    DATETIME_FIELDS,
)


class TestConvertDatetimeValue:
    """Test datetime string to datetime object conversion."""

    def test_converts_datetime_string_to_datetime_object(self):
        """Test that datetime strings are converted to datetime objects."""
        filter_array = ["publication_date", "Eq", "2023-01-15"]
        result = convert_datetime_value(filter_array)

        assert result is not None
        assert result[0] == "publication_date"
        assert result[1] == "Eq"
        assert isinstance(result[2], datetime)
        assert result[2].year == 2023
        assert result[2].month == 1
        assert result[2].day == 15

    def test_converts_datetime_with_different_operators(self):
        """Test datetime conversion works with various operators."""
        operators = ["Eq", "Gt", "Lt", "Gte", "Lte"]

        for operator in operators:
            filter_array = ["publication_date", operator, "2020-06-30"]
            result = convert_datetime_value(filter_array)

            assert result is not None
            assert result[1] == operator
            assert isinstance(result[2], datetime)

    def test_returns_none_for_non_datetime_fields(self):
        """Test that non-datetime fields are not processed."""
        filter_array = ["subject", "Eq", "2023-01-15"]
        result = convert_datetime_value(filter_array)

        assert result is None

    def test_returns_none_for_non_three_element_arrays(self):
        """Test that arrays without exactly 3 elements are not processed."""
        # Too few elements
        result = convert_datetime_value(["publication_date", "Eq"])
        assert result is None

        # Too many elements
        result = convert_datetime_value(
            ["publication_date", "Eq", "2023-01-15", "extra"]
        )
        assert result is None

        # Nested array (complex filter)
        result = convert_datetime_value(
            ["Or", [["publication_date", "Eq", "2023-01-15"]]]
        )
        assert result is None

    def test_returns_none_for_non_list_inputs(self):
        """Test that non-list inputs are not processed."""
        assert convert_datetime_value("not a list") is None
        assert convert_datetime_value(123) is None
        assert convert_datetime_value(None) is None

    def test_handles_iso_format_datetime_strings(self):
        """Test conversion of various ISO format datetime strings."""
        test_cases = [
            "2023-01-15",
            "2023-01-15T10:30:00",
            "2023-01-15T10:30:00.123456",
        ]

        for date_string in test_cases:
            filter_array = ["publication_date", "Eq", date_string]
            result = convert_datetime_value(filter_array)

            assert result is not None
            assert isinstance(result[2], datetime)

    def test_raises_error_for_invalid_datetime_string(self):
        """Test that invalid datetime strings raise ValueError."""
        filter_array = ["publication_date", "Eq", "not-a-date"]

        with pytest.raises(ValueError):
            convert_datetime_value(filter_array)

    def test_returns_none_for_null_value(self):
        """Test that a None value passes through as None (no conversion attempted)."""
        result = convert_datetime_value(["publication_date", "Eq", None])
        assert result is None

    def test_handles_z_timezone_suffix(self):
        """Test that Z-suffixed ISO strings are handled (py <3.11 quirk)."""
        filter_array = ["publication_date", "Eq", "2023-01-15T10:30:00Z"]
        result = convert_datetime_value(filter_array)

        assert result is not None
        assert isinstance(result[2], datetime)
        assert result[2].year == 2023

    def test_returns_none_for_non_string_field_or_operator(self):
        """Test that filters with non-string field names or operators are not processed."""
        # Non-string field name
        assert convert_datetime_value([123, "Eq", "2023-01-15"]) is None
        # Non-string operator
        assert convert_datetime_value(["publication_date", None, "2023-01-15"]) is None
        # Both non-string
        assert convert_datetime_value([123, 456, "2023-01-15"]) is None


class TestAddNullMatch:
    """Test null-matching wrapper for incomplete attributes."""

    def test_wraps_incomplete_attribute_with_or_null(self):
        """Test that incomplete attributes get wrapped with null matching."""
        filter_array = ["subject", "ContainsAnyToken", "poetry"]
        result = add_null_match(filter_array)

        assert result is not None
        assert result[0] == "Or"
        assert len(result[1]) == 2

        # Original filter should be preserved
        assert result[1][0] == ["subject", "ContainsAnyToken", "poetry"]

        # Null check should be added
        assert result[1][1] == ["subject", "Eq", None]

    def test_wraps_all_incomplete_attributes(self):
        """Test that all incomplete attributes are processed."""
        for attr in INCOMPLETE_ATTRIBUTES:
            filter_array = [attr, "Eq", "some_value"]
            result = add_null_match(filter_array)

            assert result is not None
            assert result[0] == "Or"
            assert result[1][0] == [attr, "Eq", "some_value"]
            assert result[1][1] == [attr, "Eq", None]

    def test_returns_none_for_complete_attributes(self):
        """Test that non-incomplete attributes are not processed."""
        filter_array = ["title", "Contains", "machine learning"]
        result = add_null_match(filter_array)

        assert result is None

    def test_returns_none_for_non_three_element_arrays(self):
        """Test that arrays without exactly 3 elements are not processed."""
        # Too few elements
        result = add_null_match(["subject", "Eq"])
        assert result is None

        # Too many elements
        result = add_null_match(["subject", "Eq", "value", "extra"])
        assert result is None

        # Nested array (complex filter)
        result = add_null_match(["Or", [["subject", "Eq", "value"]]])
        assert result is None

    def test_returns_none_for_non_list_inputs(self):
        """Test that non-list inputs are not processed."""
        assert add_null_match("not a list") is None
        assert add_null_match(123) is None
        assert add_null_match(None) is None


# Isolated stub used by TestProcessFiltersRecursively so tests are not coupled
# to the behaviour of any real processing function.
# Matches only ["TARGET", "Eq", "value"] and returns the sentinel ["PROCESSED"].
def _stub_processing_func(filter_array):
    if (
        isinstance(filter_array, list)
        and len(filter_array) == 3
        and filter_array[0] == "TARGET"
    ):
        return ["PROCESSED"]
    return None


class TestProcessFiltersRecursively:
    """Test recursive filter processing."""

    def test_applies_function_to_applicable_filters(self):
        """Test that the processing function is applied when it matches."""
        filters = ["TARGET", "Eq", "value"]
        result = process_filters_recursively(filters, _stub_processing_func)

        assert result == ["PROCESSED"]

    def test_does_not_apply_function_to_non_matching_filters(self):
        """Test that non-matching filters are returned unchanged."""
        filters = ["OTHER", "Eq", "value"]
        result = process_filters_recursively(filters, _stub_processing_func)

        assert result == filters

    def test_recursively_processes_nested_filters(self):
        """Test that nested filters are processed recursively."""
        filters = [
            "And",
            [
                ["TARGET", "Eq", "a"],
                ["OTHER", "Eq", "b"],
            ],
        ]

        result = process_filters_recursively(filters, _stub_processing_func)

        assert result[0] == "And"
        assert result[1][0] == ["PROCESSED"]  # TARGET was processed
        assert result[1][1] == ["OTHER", "Eq", "b"]  # OTHER was left unchanged

    def test_handles_deeply_nested_filters(self):
        """Test processing of deeply nested filter structures."""
        filters = [
            "Or",
            [
                ["And", [["TARGET", "Eq", "a"], ["OTHER", "Eq", "b"]]],
                ["SKIP", "Contains", "test"],
            ],
        ]

        result = process_filters_recursively(filters, _stub_processing_func)

        assert result[0] == "Or"
        assert result[1][0][0] == "And"
        assert result[1][0][1][0] == ["PROCESSED"]  # TARGET deep inside
        assert result[1][0][1][1] == ["OTHER", "Eq", "b"]  # OTHER unchanged

    def test_preserves_non_applicable_filters(self):
        """Test that filters not matching the processing function are preserved."""
        filters = ["title", "Contains", "machine learning"]
        result = process_filters_recursively(filters, _stub_processing_func)

        assert result == filters

    def test_returns_scalar_values_unchanged(self):
        """Test that scalar values (operators, field names, etc.) are not processed."""
        assert process_filters_recursively("And", _stub_processing_func) == "And"
        assert process_filters_recursively("TARGET", _stub_processing_func) == "TARGET"
        assert process_filters_recursively(123, _stub_processing_func) == 123
        assert process_filters_recursively(None, _stub_processing_func) is None
