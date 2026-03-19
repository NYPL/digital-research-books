import os
import pytest
from unittest.mock import AsyncMock, MagicMock
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion, ChatCompletionMessage
from openai.types.chat.chat_completion import Choice
from openai.types.chat.chat_completion_message_tool_call import (
    ChatCompletionMessageToolCall,
    Function,
)
from agents import Agent, Runner, RunConfig, function_tool
from agents.models.openai_chatcompletions import OpenAIChatCompletionsModel
from api.assistant.agent import update_chat, _on_max_turns


class TestAgent:
    def test_update_chat_catalog_search(self, mocker):
        """Test update_chat in catalogSearch mode returns the expected result."""

        # Mock search dependencies
        mocker.patch("api.assistant.agent.TurbopufferBackend")
        mocker.patch("api.assistant.agent.get_config")
        mocker.patch.dict(os.environ, {"GOOGLE_API_KEY": "fake-key"})

        # Mock the agent and its runner to simulate execution
        mocker.patch("api.assistant.agent.Agent")
        mock_runner = mocker.patch("api.assistant.agent.Runner")
        mock_run_result = mocker.MagicMock()
        mock_runner.run_sync.return_value = mock_run_result

        # Mock prompt template rendering
        mock_template = mocker.patch("api.assistant.agent.Template")
        mock_template_instance = mocker.MagicMock()
        mock_template.return_value = mock_template_instance
        mock_template_instance.render.return_value = "system prompt"

        # Execute a catalog search using a simple user prompt
        conversation = [{"role": "user", "content": "Some query"}]
        result = update_chat(conversation, "catalogSearch")

        # Verify result and that the runner was called just once
        assert result == mock_run_result
        mock_runner.run_sync.assert_called_once()


class TestOnMaxTurnsUnit:
    @pytest.mark.asyncio
    async def test_runner_calls_on_max_turns_and_returns_handler_output(self, mocker):
        """
        Real Runner.run with max_turns=2, a mocked LLM client, and _on_max_turns as handler.
        Verifies:
        1. final_output equals the mocked graceful response string from _on_max_turns.
        2. raw_responses has exactly 2 entries (one per agent-loop LLM call).
        3. No MaxTurnsExceeded is raised.
        """

        # --- Build mock ChatCompletion objects ---

        # Tool call response: LLM asks to call dummy_tool (triggers re-run, not final output)
        tool_call_response = ChatCompletion(
            id="fake-tool-call-id",
            created=0,
            model="test-model",
            object="chat.completion",
            choices=[
                Choice(
                    finish_reason="tool_calls",
                    index=0,
                    message=ChatCompletionMessage(
                        role="assistant",
                        content=None,
                        tool_calls=[
                            ChatCompletionMessageToolCall(
                                id="call-1",
                                type="function",
                                function=Function(
                                    name="dummy_tool",
                                    arguments='{"query": "test"}',
                                ),
                            )
                        ],
                    ),
                )
            ],
        )

        # Graceful response: returned by _on_max_turns' direct client call (3rd call)
        graceful_response = ChatCompletion(
            id="fake-graceful-id",
            created=0,
            model="test-model",
            object="chat.completion",
            choices=[
                Choice(
                    finish_reason="stop",
                    index=0,
                    message=ChatCompletionMessage(
                        role="assistant",
                        content="MOCKED_GRACEFUL_RESPONSE",
                        tool_calls=None,
                    ),
                )
            ],
        )

        # --- Mock LLM client ---
        # Call 1: agent loop turn 1 → tool_call_response
        # Call 2: agent loop turn 2 → tool_call_response
        # Call 3: _on_max_turns direct call → graceful_response
        mock_client = AsyncMock(spec=AsyncOpenAI)
        mock_client.chat.completions.create = AsyncMock(
            side_effect=[tool_call_response, tool_call_response, graceful_response]
        )
        # Required by OpenAIChatCompletionsModel for base_url logging
        mock_client.base_url = "https://fake.api/"

        # --- Build agent with inline dummy tool ---
        @function_tool
        def dummy_tool(query: str) -> str:
            return "dummy result"

        model = OpenAIChatCompletionsModel(
            model="test-model", openai_client=mock_client
        )
        agent = Agent(
            name="Test Agent",
            model=model,
            instructions="You are a test assistant.",
            tools=[dummy_tool],
        )

        # --- Run ---
        result = await Runner.run(
            agent,
            "find books",
            max_turns=2,
            error_handlers={"max_turns": _on_max_turns},
            run_config=RunConfig(tracing_disabled=True),
        )

        # --- Assertions ---
        assert result.final_output == "MOCKED_GRACEFUL_RESPONSE"
        assert len(result.raw_responses) == 2

    @pytest.mark.asyncio
    async def test_on_max_turns_awaits_get_system_prompt(self):
        """
        Calls _on_max_turns directly and asserts get_system_prompt was awaited.
        assert_awaited_once() fails if the coroutine was called but not awaited,
        catching a missing `await` before the .format() call.
        """
        mock_agent = MagicMock()
        mock_agent.get_system_prompt = AsyncMock(return_value="SYSTEM PROMPT")
        mock_agent.model._client.chat.completions.create = AsyncMock()

        mock_data = MagicMock()
        mock_data.run_data.last_agent = mock_agent
        mock_data.run_data.history = []

        await _on_max_turns(mock_data)

        mock_agent.get_system_prompt.assert_awaited_once()
