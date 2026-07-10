from unittest.mock import patch

from api.assistant.snippets import format_conversation_history


class TestFormatConversationHistory:
    @staticmethod
    def _user_msg(text: str) -> dict:
        return {"type": "message", "role": "user", "content": text}

    @staticmethod
    def _assistant_msg(text: str) -> dict:
        return {"type": "message", "role": "assistant", "content": text}

    @staticmethod
    def _tool_call(
        call_id: str, name: str = "search", arguments: str = '{"q": "x"}'
    ) -> dict:
        return {
            "type": "function_call",
            "call_id": call_id,
            "name": name,
            "arguments": arguments,
        }

    @staticmethod
    def _tool_output(call_id: str, output: str = "<results/>") -> dict:
        return {
            "type": "function_call_output",
            "call_id": call_id,
            "output": output,
        }

    @patch("api.assistant.snippets.get_result_count", return_value=3)
    def test_happy_path_user_tool_call_and_output(self, mock_get_result_count):
        """User message, assistant message, tool call, and tool output are all formatted."""
        items = [
            self._user_msg("Find books about whales."),
            self._assistant_msg("Sure, let me search for that."),
            self._tool_call("call_1", name="search", arguments='{"q": "whales"}'),
            self._tool_output("call_1", output="<results><edition/></results>"),
        ]

        result = format_conversation_history(items)

        assert "User: Find books about whales." in result
        assert "Assistant: Sure, let me search for that." in result
        assert 'Tool call [search]: {"q": "whales"}' in result
        assert "[Tool Output: search]\n3 results returned" in result

    @patch("api.assistant.snippets.get_result_count", return_value=0)
    def test_final_tool_call_without_matching_output(self, mock_get_result_count):
        """A tool call with no matching function_call_output is included without a Tool Output line."""
        items = [
            self._user_msg("Find books about whales."),
            self._tool_call("call_1", name="search", arguments='{"q": "whales"}'),
        ]

        result = format_conversation_history(items)

        assert 'Tool call [search]: {"q": "whales"}' in result
        assert "[Tool Output" not in result

    @patch("api.assistant.snippets.get_result_count", return_value=5)
    def test_only_final_tool_call_of_multiple_is_formatted(self, mock_get_result_count):
        """When multiple tool calls happen back-to-back, only the final call/output pair is kept."""
        items = [
            self._tool_call("call_1", name="search", arguments='{"q": "first"}'),
            self._tool_output("call_1", output="<results><edition/></results>"),
            self._tool_call("call_2", name="search", arguments='{"q": "second"}'),
            self._tool_output("call_2", output="<results><edition/></results>"),
        ]

        result = format_conversation_history(items)

        assert '{"q": "first"}' not in result
        assert '{"q": "second"}' in result
        assert result.count("Tool call [search]:") == 1
        assert result.count("[Tool Output: search]") == 1
