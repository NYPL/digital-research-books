import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from agents import Agent, RunConfig, Runner, function_tool
from agents.models.openai_chatcompletions import OpenAIChatCompletionsModel
from api.assistant.agent import (
    _MAX_TURNS_SYSTEM_PROMPT,
    _on_max_turns,
    search_book,
    search_catalog,
    TOOL_ERROR_PREFIX,
    update_chat,
)
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion, ChatCompletionMessage
from openai.types.chat.chat_completion import Choice
from openai.types.chat.chat_completion_message_tool_call import (
    ChatCompletionMessageToolCall,
    Function,
)

from tests.factories import make_chunk_doc

from .conftest import make_search_book_tool_context, make_search_catalog_tool_context


def mock_update_chat_env(mocker):
    """Patch all external dependencies of update_chat and return the mock Runner."""
    mocker.patch(
        "api.assistant.agent.get_index_config",
        return_value={
            "embedder": mocker.MagicMock(),
            "backend": mocker.MagicMock(),
        },
    )
    mocker.patch("api.assistant.agent.Agent")
    mock_runner = mocker.patch("api.assistant.agent.Runner")
    mock_run_result = MagicMock()
    mock_runner.run = AsyncMock(return_value=mock_run_result)

    mock_template = mocker.patch("api.assistant.agent.Template")
    mock_template_instance = MagicMock()
    mock_template.return_value = mock_template_instance
    mock_template_instance.render.return_value = "system prompt"

    return mock_runner, mock_run_result


class TestUpdateChat:
    def test_update_chat_catalog_search(self, mocker):
        """Test update_chat in catalogSearch mode returns value from Runner.run()."""
        mock_runner, mock_run_result = mock_update_chat_env(mocker)
        mock_session = MagicMock()

        result = update_chat("Some query", "catalogSearch", mock_session)

        assert result == mock_run_result
        mock_runner.run.assert_called_once()


class TestSearchToolInvocation:
    """
    Tests for bugs in the tool's internal logic regardless of whether the LLM
    ever chooses to call the tool. Calls search_book/search_catalog directly via
    on_invoke_tool(). Mocks external I/O dependencies via mock_search_backend().
    """

    @pytest.fixture
    def chunk_doc(self):
        return make_chunk_doc(
            text="Merchants traded goods along the Missouri river.",
            title="The Missouri Merchant",
            edition_id=42,
            barcode="00000000000042",
            author=["Jane Doe"],
            subject=["History"],
        )

    @pytest.mark.asyncio
    async def test_search_book_on_invoke_tool_returns_results(
        self, mocker, mock_search_backend, chunk_doc
    ):
        mock_search_backend([chunk_doc])

        tool_call_id = "call-book-1"
        ctx = make_search_book_tool_context(
            tool_call_id=tool_call_id,
            edition_id=42,
            ranking_query="merchants",
            frbr_fields={
                "title": "The Missouri Merchant",
                "author_names": "Jane Doe",
                "subject_list": "History",
                "pub_date": "1919",
                "publisher_names": "Test Publisher",
                "language_list": "English",
            },
        )

        result = await search_book.on_invoke_tool(ctx, ctx.tool_arguments)

        assert "error" not in result.lower()
        assert chunk_doc.text in result
        assert tool_call_id in ctx.context.search_results

    @pytest.mark.asyncio
    async def test_search_catalog_on_invoke_tool_returns_results(
        self, mocker, mock_search_backend, chunk_doc
    ):
        mock_search_backend([chunk_doc])

        tool_call_id = "call-catalog-1"
        ctx = make_search_catalog_tool_context(
            tool_call_id=tool_call_id, ranking_query="merchants"
        )

        result = await search_catalog.on_invoke_tool(ctx, ctx.tool_arguments)

        assert "error" not in result.lower()
        assert chunk_doc.text in result
        assert tool_call_id in ctx.context.search_results


class TestSearchToolErrorHandling:
    """
    Verifies that search_catalog and search_book surface tool execution errors
    with the agents SDK's default tool-error prefix (TOOL_ERROR_PREFIX), rather
    than raising or returning some other shape.

    This matters because several pieces of code detect tool errors
    by checking `tool_call_output.startswith(TOOL_ERROR_PREFIX)` — if the SDK's
    error format ever changes, that guard silently stops working.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "tool, make_context",
        [
            (search_catalog, make_search_catalog_tool_context),
            (search_book, make_search_book_tool_context),
        ],
        ids=["search_catalog", "search_book"],
    )
    async def test_tool_error_returns_tool_error_prefix(
        self, mocker, tool, make_context
    ):
        mock_hybrid_search = mocker.patch(
            "api.assistant.agent.hybrid_search",
            side_effect=RuntimeError("backend unreachable"),
        )
        ctx = make_context()

        result = await tool.on_invoke_tool(ctx, ctx.tool_arguments)

        assert result.startswith(TOOL_ERROR_PREFIX)
        mock_hybrid_search.assert_called_once()


class TestOnMaxTurns:
    def make_mock_data(self, agent, history=None):
        mock_data = MagicMock()
        mock_data.run_data.last_agent = agent
        mock_data.run_data.history = history if history is not None else []
        return mock_data

    @pytest.fixture
    def mock_client(self):
        client = AsyncMock()
        client.chat.completions.create = AsyncMock(
            return_value=MagicMock(choices=[MagicMock(message=MagicMock(content="ok"))])
        )
        return client

    @pytest.fixture
    def mock_agent(self, mock_client):
        agent = MagicMock()
        agent.model._client = mock_client
        agent.model.model = "test-model"
        agent.get_system_prompt = AsyncMock(return_value="STUB_AGENT_INSTRUCTIONS")
        return agent

    @pytest.mark.asyncio
    async def test_runner_calls_on_max_turns(self, mocker):
        """
        Real Runner.run with max_turns=2, a mocked LLM client, and _on_max_turns as handler.
        Verifies:
        1. final_output equals the mocked response string from the LLM call in
        _on_max_turns (this expects _on_max_turns to call an LLM using the same
        client as the main Runner)
        2. raw_responses has exactly 2 entries (i.e. the agent-loop included 2
        LLM calls only).
        3. No MaxTurnsExceeded is raised.
        """
        # NOTE: this test is considerable asserts behavior internal to the
        # openai agents SDK and is covered significantly by assertions in
        # test_on_max_turns.py:TestOnMaxTurns:test_update_chat_max_turns_graceful_response

        # --- mock ChatCompletion response objects ---

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

        max_turns_response_content = "MOCKED_GRACEFUL_RESPONSE"

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
                        content=max_turns_response_content,
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

        model = OpenAIChatCompletionsModel(
            model="test-model", openai_client=mock_client
        )

        @function_tool
        def dummy_tool(query: str) -> str:
            return "dummy result"

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
    async def test_on_max_turns_awaits_get_system_prompt(self, mock_agent):
        """
        Verifies get_system_prompt was awaited inside _on_max_turns.
        Fails if the async get_system_prompt() was called not awaited,
        catching a missing `await` before the .format() call.
        """
        await _on_max_turns(self.make_mock_data(mock_agent))

        mock_agent.get_system_prompt.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_on_max_turns_system_prompt_contains_agent_instructions(
        self, mock_client, mock_agent
    ):
        """System message must embed the agent's original instructions inside _MAX_TURNS_SYSTEM_PROMPT."""
        await _on_max_turns(self.make_mock_data(mock_agent))

        system_content = mock_client.chat.completions.create.call_args.kwargs[
            "messages"
        ][0]["content"]
        assert "STUB_AGENT_INSTRUCTIONS" in system_content
        assert system_content == _MAX_TURNS_SYSTEM_PROMPT.format(
            agent_system_prompt="STUB_AGENT_INSTRUCTIONS"
        )

    @pytest.mark.asyncio
    async def test_on_max_turns_uses_conversation_history(
        self, mocker, mock_client, mock_agent
    ):
        """History items must appear after the system prompt, in order."""
        history_messages = [
            {"role": "user", "content": "find me a book"},
            {"role": "assistant", "content": "searching..."},
        ]
        mocker.patch(
            "api.assistant.agent.Converter.items_to_messages",
            return_value=history_messages,
        )

        await _on_max_turns(
            self.make_mock_data(mock_agent, history=[object(), object()])
        )

        sent_messages = mock_client.chat.completions.create.call_args.kwargs["messages"]
        assert sent_messages[0]["role"] == "system"
        assert sent_messages[1:] == history_messages
