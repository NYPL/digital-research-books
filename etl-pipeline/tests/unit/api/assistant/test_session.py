from unittest.mock import MagicMock

from api.assistant.session import get_new_items_with_ids


class TestGetNewItemsWithIds:
    @staticmethod
    def _make_run_item(input_item: dict):
        """Wrap a raw input dict as a mock object with .to_input_item()."""
        mock_item = MagicMock(spec_set=["to_input_item"])
        mock_item.to_input_item.return_value = input_item
        return mock_item

    @staticmethod
    def _make_session(inserted: list[tuple[int, dict]]) -> MagicMock:
        """An object with an .inserted_items attr"""
        session = MagicMock()
        session.inserted_items = inserted
        return session

    @staticmethod
    def _make_run_result(new_items) -> MagicMock:
        """An object with a .new_items attr"""
        run_result = MagicMock()
        run_result.new_items = new_items
        return run_result

    def test_returns_new_items_with_db_ids(self):
        """Each new_item that appears in inserted_items gets its db_id attached."""
        agent_msg = {"role": "assistant", "content": "Here are some books."}
        input_msg = {"role": "user", "content": "Find books about climate."}

        session = self._make_session([(1, input_msg), (2, agent_msg)])
        run_result = self._make_run_result([self._make_run_item(agent_msg)])

        result = get_new_items_with_ids(run_result, session)

        assert len(result) == 1
        assert result[0] == {"db_id": 2, **agent_msg}

    def test_empty_new_items(self):
        session = self._make_session([(1, {"role": "user", "content": "hi"})])
        run_result = self._make_run_result([])

        result = get_new_items_with_ids(run_result, session)

        assert result == []

    def test_empty_inserted_items(self):
        agent_msg = {"role": "assistant", "content": "response"}
        session = self._make_session([])
        run_result = self._make_run_result([self._make_run_item(agent_msg)])

        result = get_new_items_with_ids(run_result, session)

        assert result == []

    def test_duplicate_items_use_first_available_db_id(self):
        """When the same content appears twice, each occurrence gets its own db_id."""
        msg = {"role": "assistant", "content": "same content"}
        session = self._make_session([(10, msg), (11, msg)])
        run_result = self._make_run_result(
            [self._make_run_item(msg), self._make_run_item(msg)]
        )

        result = get_new_items_with_ids(run_result, session)

        assert len(result) == 2
        assert result[0]["db_id"] == 10
        assert result[1]["db_id"] == 11
