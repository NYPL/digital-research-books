import pytest

from utils.common import require_env


class TestReadEnv:
    """Test cases for the require_env function"""

    def test_require_env_variable_exists(self, mocker):
        """Test reading an environment variable that exists"""
        mocker.patch.dict("os.environ", {"TEST_VAR": "test_value"})

        result = require_env("TEST_VAR")

        assert result == "test_value"

    def test_require_env_variable_missing(self, mocker):
        """Test reading an environment variable that doesn't exist raises ValueError"""
        mocker.patch.dict("os.environ", {}, clear=True)

        with pytest.raises(ValueError) as exc_info:
            require_env("MISSING_VAR")

    def test_require_env_empty_string_value(self, mocker):
        """Test reading an environment variable with an empty string value"""
        mocker.patch.dict("os.environ", {"EMPTY_VAR": ""})

        result = require_env("EMPTY_VAR")

        assert result == ""

    def test_require_env_whitespace_value(self, mocker):
        """Test reading an environment variable with whitespace value"""
        mocker.patch.dict("os.environ", {"WHITESPACE_VAR": "  spaces  "})

        result = require_env("WHITESPACE_VAR")

        assert result == "  spaces  "

    def test_require_env_numeric_string_value(self, mocker):
        """Test reading an environment variable with numeric string value"""
        mocker.patch.dict("os.environ", {"NUMERIC_VAR": "12345"})

        result = require_env("NUMERIC_VAR")

        assert result == "12345"
        assert isinstance(result, str)

    def test_require_env_special_characters(self, mocker):
        """Test reading an environment variable with special characters"""
        special_value = "test@#$%^&*()_+={}[]|\\:;\"'<>,.?/~`"
        mocker.patch.dict("os.environ", {"SPECIAL_VAR": special_value})

        result = require_env("SPECIAL_VAR")

        assert result == special_value

    def test_require_env_multiline_value(self, mocker):
        """Test reading an environment variable with newlines"""
        multiline_value = "line1\nline2\nline3"
        mocker.patch.dict("os.environ", {"MULTILINE_VAR": multiline_value})

        result = require_env("MULTILINE_VAR")

        assert result == multiline_value

    def test_require_env_case_sensitive(self, mocker):
        """Test that environment variable names are case-sensitive"""
        # NOTE: case sensitivity does not apply to os.environ on windows
        mocker.patch.dict("os.environ", {"test_var": "lowercase"}, clear=True)

        # Should find the lowercase version
        result = require_env("test_var")
        assert result == "lowercase"

        # Should not find the uppercase version
        with pytest.raises(ValueError) as exc_info:
            require_env("TEST_VAR")
