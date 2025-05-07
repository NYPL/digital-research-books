import pytest

from mappings.base_mapping import CustomFormatter


class TestCustomFormatter:
    @pytest.fixture
    def testFormatter(self):
        return CustomFormatter()

    def test_format_positional_success(self, testFormatter):
        assert testFormatter.format("{0} {1}", "hello", "world") == "hello world"

    def test_format_positional_missing(self, testFormatter):
        assert testFormatter.format("{0} {1}", "hello") == "hello "

    def test_format_positional_only_empty(self, testFormatter):
        with pytest.raises(IndexError):
            testFormatter.format("{0} {1}", "", "")

    def test_format_named_success(self, testFormatter):
        assert (
            testFormatter.format("{greeting} {place}", greeting="hello", place="world")
            == "hello world"
        )

    def test_format_named_key_missing(self, testFormatter):
        assert (
            testFormatter.format("{greeting} ", greeting="hello", place="world")
            == "hello "
        )

    def test_format_named_value_missing(self, testFormatter):
        assert testFormatter.format("{greeting} {place}", place="world") == " world"

    def test_format_named_only_empty(self, testFormatter):
        with pytest.raises(KeyError):
            testFormatter.format("{greeting} {place}", greeting="", place="")
