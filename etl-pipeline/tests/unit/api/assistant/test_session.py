from unittest.mock import MagicMock

import pytest
from agents.items import ToolApprovalItem

from api.assistant.agent import get_new_items_with_ids


class TestGetNewItemsWithIds:
    @staticmethod
    def _make_run_item(input_item: dict):
        """Wrap a raw input dict as a mock object with .to_input_item()."""
        mock_item = MagicMock(spec_set=["to_input_item"])
        mock_item.to_input_item.return_value = input_item
        return mock_item

    @staticmethod
    def _make_run_result(new_items) -> MagicMock:
        run_result = MagicMock()
        run_result.new_items = new_items
        return run_result

    def test_matching_item_gets_db_id(self):
        """An assistant message matching a session_message_item receives db_id."""
        agent_msg = {
            "role": "assistant",
            "content": "Here are some books.",
            "type": "message",
        }
        session_msg = {"db_id": 42, **agent_msg}

        run_result = self._make_run_result([self._make_run_item(agent_msg)])

        result = get_new_items_with_ids(run_result, [session_msg])

        assert len(result) == 1
        assert result[0] == {"db_id": 42, **agent_msg}

    def test_non_matching_item_returned_without_db_id(self):
        """A tool-call item not in session_message_items is returned without db_id."""
        tool_call = {"role": "assistant", "content": None, "type": "tool_call"}
        agent_msg = {"role": "assistant", "content": "Done.", "type": "message"}
        session_msg = {"db_id": 7, **agent_msg}

        run_result = self._make_run_result(
            [self._make_run_item(tool_call), self._make_run_item(agent_msg)]
        )

        result = get_new_items_with_ids(run_result, [session_msg])

        assert len(result) == 2
        assert result[0] == tool_call
        assert "db_id" not in result[0]
        assert result[1] == {"db_id": 7, **agent_msg}

    def test_empty_new_items(self):
        run_result = self._make_run_result([])
        result = get_new_items_with_ids(
            run_result,
            [{"db_id": 1, "role": "assistant", "content": "hi", "type": "message"}],
        )
        assert result == []

    def test_empty_session_message_items(self):
        """All new items pass through without db_id when session_message_items is empty."""
        agent_msg = {"role": "assistant", "content": "response", "type": "message"}
        run_result = self._make_run_result([self._make_run_item(agent_msg)])

        result = get_new_items_with_ids(run_result, [])

        assert result == [agent_msg]
        assert "db_id" not in result[0]

    def test_duplicate_items_use_first_available_db_id(self):
        """When the same content appears twice, each occurrence gets its own db_id."""
        msg = {"role": "assistant", "content": "same content", "type": "message"}
        session_msgs = [{"db_id": 10, **msg}, {"db_id": 11, **msg}]
        run_result = self._make_run_result(
            [self._make_run_item(msg), self._make_run_item(msg)]
        )

        result = get_new_items_with_ids(run_result, session_msgs)

        assert len(result) == 2
        assert result[0]["db_id"] == 10
        assert result[1]["db_id"] == 11

    def test_tool_approval_item_excluded(self):
        """ToolApprovalItem entries are dropped from the result."""
        tool_approval = MagicMock(spec=ToolApprovalItem)
        agent_msg = {"role": "assistant", "content": "ok", "type": "message"}
        session_msgs = [{"db_id": 5, **agent_msg}]

        run_result = self._make_run_result(
            [tool_approval, self._make_run_item(agent_msg)]
        )

        result = get_new_items_with_ids(run_result, session_msgs)

        assert len(result) == 1
        assert result[0]["db_id"] == 5
