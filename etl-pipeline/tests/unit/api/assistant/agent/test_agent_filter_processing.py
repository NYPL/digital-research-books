"""
Unit tests for agent filter post-processing functions.

Tests the filter transformation pipeline including datetime conversion
and null-matching for incomplete attributes.
"""

import pytest
from datetime import datetime
from api.assistant.agent import (
    transform_datetime,
    transform_incomplete,
    recurse_filters,
    apply_filter_transforms,
    INCOMPLETE_ATTRIBUTES,
    DATETIME_ATTRIBUTES,
)


class TestConvertDatetimeValue:
    """Test datetime string to datetime object conversion."""

    def test_converts_datetime_string_to_datetime_object(self):
        """Test that datetime strings are converted to datetime objects."""
        filter_array = ["publication_date", "Eq", "2023-01-15"]
        result = transform_datetime(filter_array)

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
            result = transform_datetime(filter_array)

            assert result is not None
            assert result[1] == operator
            assert isinstance(result[2], datetime)

    def test_returns_unchanged_for_non_datetime_fields(self):
        """Test that non-datetime fields are returned unchanged."""
        filter_array = ["subject", "Eq", "2023-01-15"]
        result = transform_datetime(filter_array)

        assert result == filter_array

    def test_returns_unchanged_for_non_three_element_arrays(self):
        """Test that arrays without exactly 3 elements are returned unchanged."""
        # Too few elements
        f = ["publication_date", "Eq"]
        assert transform_datetime(f) == f

        # Too many elements
        f = ["publication_date", "Eq", "2023-01-15", "extra"]
        assert transform_datetime(f) == f

        # Nested array (complex filter)
        f = ["Or", [["publication_date", "Eq", "2023-01-15"]]]
        assert transform_datetime(f) == f

    def test_returns_unchanged_for_non_list_inputs(self):
        """Test that non-list inputs are returned unchanged."""
        assert transform_datetime("not a list") == "not a list"
        assert transform_datetime(123) == 123
        assert transform_datetime(None) is None

    def test_handles_iso_format_datetime_strings(self):
        """Test conversion of various ISO format datetime strings."""
        test_cases = [
            "2023-01-15",
            "2023-01-15T10:30:00",
            "2023-01-15T10:30:00.123456",
        ]

        for date_string in test_cases:
            filter_array = ["publication_date", "Eq", date_string]
            result = transform_datetime(filter_array)

            assert result is not None
            assert isinstance(result[2], datetime)

    def test_raises_error_for_invalid_datetime_string(self):
        """Test that invalid datetime strings raise ValueError."""
        filter_array = ["publication_date", "Eq", "not-a-date"]

        with pytest.raises(ValueError):
            transform_datetime(filter_array)

    def test_returns_unchanged_for_null_value(self):
        """Test that a null attribute value passes the filter through unchanged (no conversion attempted)."""
        f = ["publication_date", "Eq", None]
        assert transform_datetime(f) == f

    def test_handles_z_timezone_suffix(self):
        """Test that Z-suffixed ISO strings are handled (py <3.11 quirk)."""
        filter_array = ["publication_date", "Eq", "2023-01-15T10:30:00Z"]
        result = transform_datetime(filter_array)

        assert result is not None
        assert isinstance(result[2], datetime)
        assert result[2].year == 2023

    def test_returns_unchanged_for_non_string_field_or_operator(self):
        """Test that filters with non-string field names or operators are returned unchanged."""
        # Non-string field name
        f = [123, "Eq", "2023-01-15"]
        assert transform_datetime(f) == f
        # Non-string operator
        f = ["publication_date", None, "2023-01-15"]
        assert transform_datetime(f) == f
        # Both non-string
        f = [123, 456, "2023-01-15"]
        assert transform_datetime(f) == f


class TestAddNullMatch:
    """Test null-matching wrapper for incomplete attributes."""

    def test_wraps_incomplete_attribute_with_or_null(self):
        """Test that incomplete attributes get wrapped with null matching."""
        filter_array = ["subject", "ContainsAnyToken", "poetry"]
        result = transform_incomplete(filter_array)

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
            result = transform_incomplete(filter_array)

            assert result is not None
            assert result[0] == "Or"
            assert result[1][0] == [attr, "Eq", "some_value"]
            assert result[1][1] == [attr, "Eq", None]

    def test_returns_unchanged_for_complete_attributes(self):
        """Test that non-incomplete attributes are returned unchanged."""
        filter_array = ["title", "Contains", "machine learning"]
        result = transform_incomplete(filter_array)

        assert result == filter_array

    def test_returns_unchanged_for_non_three_element_arrays(self):
        """Test that arrays without exactly 3 elements are returned unchanged."""
        # Too few elements
        f = ["subject", "Eq"]
        assert transform_incomplete(f) == f

        # Too many elements
        f = ["subject", "Eq", "value", "extra"]
        assert transform_incomplete(f) == f

        # Nested array (complex filter)
        f = ["Or", [["subject", "Eq", "value"]]]
        assert transform_incomplete(f) == f

    def test_returns_unchanged_for_non_list_inputs(self):
        """Test that non-list inputs are returned unchanged."""
        assert transform_incomplete("not a list") == "not a list"
        assert transform_incomplete(123) == 123
        assert transform_incomplete(None) is None


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
    return filter_array


class TestProcessFiltersRecursively:
    """Test recursive filter processing."""

    def test_applies_function_to_applicable_filters(self):
        """Test that the processing function is applied when it matches."""
        filters = ["TARGET", "Eq", "value"]
        result = recurse_filters(filters, _stub_processing_func)

        assert result == ["PROCESSED"]

    def test_does_not_apply_function_to_non_matching_filters(self):
        """Test that non-matching filters are returned unchanged."""
        filters = ["OTHER", "Eq", "value"]
        result = recurse_filters(filters, _stub_processing_func)

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

        result = recurse_filters(filters, _stub_processing_func)

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

        result = recurse_filters(filters, _stub_processing_func)

        assert result[0] == "Or"
        assert result[1][0][0] == "And"
        assert result[1][0][1][0] == ["PROCESSED"]  # TARGET deep inside
        assert result[1][0][1][1] == ["OTHER", "Eq", "b"]  # OTHER unchanged

    def test_preserves_non_applicable_filters(self):
        """Test that filters not matching the processing function are preserved."""
        filters = ["title", "Contains", "machine learning"]
        result = recurse_filters(filters, _stub_processing_func)

        assert result == filters

    def test_raises_for_scalar_input(self):
        """Test that scalar values (non-list inputs) raise ValueError."""
        with pytest.raises(ValueError):
            recurse_filters("And", _stub_processing_func)
        with pytest.raises(ValueError):
            recurse_filters("TARGET", _stub_processing_func)
        with pytest.raises(ValueError):
            recurse_filters(123, _stub_processing_func)
        with pytest.raises(ValueError):
            recurse_filters(None, _stub_processing_func)

    def test_handles_not_operator(self):
        """Test that Not operator recurses into its single child filter."""
        filters = ["Not", ["TARGET", "Eq", "value"]]
        result = recurse_filters(filters, _stub_processing_func)

        assert result[0] == "Not"
        assert result[1] == ["PROCESSED"]
