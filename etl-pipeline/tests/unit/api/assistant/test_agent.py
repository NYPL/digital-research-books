from api.assistant.agent import update_chat


class TestAgent:
    def test_update_chat_catalog_search(self, mocker):
        """Test update_chat in catalogSearch mode returns the expected result."""

        # Mock search dependencies
        mocker.patch('api.assistant.agent.Searcher')
        mocker.patch('api.assistant.agent.GoogleEmbedder')

        # Mock the agent and its runner to simulate execution
        mocker.patch('api.assistant.agent.Agent')
        mock_runner = mocker.patch('api.assistant.agent.Runner')
        mock_run_result = mocker.MagicMock()
        mock_runner.run_sync.return_value = mock_run_result

        # Mock prompt template rendering
        mock_template = mocker.patch('api.assistant.agent.Template')
        mock_template_instance = mocker.MagicMock()
        mock_template.return_value = mock_template_instance
        mock_template_instance.render.return_value = "system prompt"

        # Execute a catalog search using a simple user prompt
        conversation = [{"role": "user", "content": "Some query"}]
        result = update_chat(conversation, "catalogSearch")

        # Verify result and that the runner was called just once
        assert result == mock_run_result
        mock_runner.run_sync.assert_called_once()
