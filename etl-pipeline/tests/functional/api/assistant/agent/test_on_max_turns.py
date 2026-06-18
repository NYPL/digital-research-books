import pytest
from agents.memory import SQLiteSession
from api.assistant.agent import update_chat


class TestOnMaxTurns:
    @pytest.mark.asyncio
    async def test_update_chat_max_turns_graceful_response(self, mocker):
        """
        Test graceful response when max turns are exceeded.

        We ensure max_turn are exceeded by setting max_turns=2 on the agent run,
        mocking the search to raise an error requesting retry and using
        a real LLM, expecting it to react by repeatedly calling the search tool.
        Expected flow: always raise error in search tool → LLM retries →
        max_turns exhausted → _on_max_turns handler fires → graceful str response returned.

        Verifies:
        1. No MaxTurnsExceeded is raised.
        2. final_output is a non-empty string (graceful LLM response).
        3. raw_responses has exactly 2 entries (max_turns agent-loop LLM calls).
        """
        # Prevent real backend initialization
        mocker.patch("api.assistant.agent.TurbopufferBackend")

        # Prevent real Google Embedder API calls; embed_query returns a mock (hybrid_search
        # raises before it's used, so the return value doesn't matter)
        mocker.patch("api.assistant.agent.GoogleEmbedder")

        # Force search tool to always fail → triggers LLM retry
        mocker.patch(
            "api.assistant.agent.hybrid_search",
            side_effect=RuntimeError("Search unavailable, please retry."),
        )

        # Mock SQLAlchemySession with in-memory sqlite session
        session = SQLiteSession("test")

        run_result = await update_chat(
            message="find me books about climate change",
            conversation_type="catalogSearch",
            session=session,
            max_turns=2,
        )

        assert isinstance(run_result.final_output, str)
        assert len(run_result.final_output) > 0
        assert len(run_result.raw_responses) == 2
