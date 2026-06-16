import asyncio
import pytest
from typing import Literal
from unittest.mock import AsyncMock
from agents import Agent, Runner, RunConfig
from agents.items import TResponseInputItem, ToolApprovalItem
from agents.models.openai_chatcompletions import OpenAIChatCompletionsModel
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion, ChatCompletionMessage
from openai.types.chat.chat_completion import Choice


# Cast the item dict as typed
def _msg(role: Literal["user", "assistant"], content: str) -> TResponseInputItem:
    return {"role": role, "content": content}


# Inspo from: https://github.com/openai/openai-agents-python/blob/main/tests/extensions/memory/test_sqlalchemy_session.py
class TestCustomSQLAlchemySessionAddItems:
    @pytest.mark.asyncio
    async def test_add_items_persists_to_db(self, test_session):
        """Items passed to add_items() are retrievable via get_items() in insertion order."""
        items = [
            _msg("user", "What books do you have?"),
            _msg("assistant", "Here are some suggestions."),
        ]

        await test_session.add_items(items)

        stored = await test_session.get_items()
        assert stored == items

    @pytest.mark.asyncio
    async def test_add_items_creates_session_row(self, test_session):
        """add_items() creates row in agent_sessions if it does not yet exist."""
        from sqlalchemy import select

        await test_session.add_items([_msg("user", "hello")])

        async with test_session._session_factory() as sess:
            result = await sess.execute(
                select(test_session._sessions.c.session_id).where(
                    test_session._sessions.c.session_id == test_session.session_id
                )
            )
            assert result.scalar_one_or_none() == test_session.session_id

    @pytest.mark.asyncio
    async def test_add_items_empty_list_is_noop(self, test_session):
        """add_items([]) must not modify inserted_items or the database."""
        await test_session.add_items([])

        assert test_session.inserted_items == []
        stored = await test_session.get_items()
        assert stored == []

    @pytest.mark.asyncio
    async def test_add_items_second_call_does_not_create_duplicate_session_row(
        self, test_session
    ):
        """Second add_items() call must not raise IntegrityError or duplicate the agent_sessions row."""
        from sqlalchemy import func, select

        await test_session.add_items([_msg("user", "first")])
        await test_session.add_items([_msg("user", "second")])

        async with test_session._session_factory() as sess:
            result = await sess.execute(
                select(func.count())
                .select_from(test_session._sessions)
                .where(test_session._sessions.c.session_id == test_session.session_id)
            )
            assert result.scalar() == 1

        stored = await test_session.get_items()
        assert len(stored) == 2

    @pytest.mark.asyncio
    async def test_add_items_updated_at_is_touched(self, test_session):
        """add_items() updates the updated_at timestamp on the session row."""
        from sqlalchemy import select

        await test_session.add_items([_msg("user", "first")])

        async with test_session._session_factory() as sess:
            result = await sess.execute(
                select(test_session._sessions.c.updated_at).where(
                    test_session._sessions.c.session_id == test_session.session_id
                )
            )
            first_updated_at = result.scalar_one()

        await asyncio.sleep(0.05)
        await test_session.add_items([_msg("user", "second")])

        async with test_session._session_factory() as sess:
            result = await sess.execute(
                select(test_session._sessions.c.updated_at).where(
                    test_session._sessions.c.session_id == test_session.session_id
                )
            )
            second_updated_at = result.scalar_one()

        assert second_updated_at > first_updated_at

    @pytest.mark.asyncio
    async def test_add_items_concurrent_first_write_does_not_race(self, test_session):
        """Concurrent first writes should not race parent session creation."""
        submitted = [f"msg-{i}" for i in range(10)]

        async def worker(content: str) -> None:
            await test_session.add_items([_msg("user", content)])

        results = await asyncio.gather(
            *(worker(content) for content in submitted),
            return_exceptions=True,
        )

        assert [r for r in results if isinstance(r, Exception)] == []

        stored = await test_session.get_items()
        assert len(stored) == len(
            submitted
        )  # exact equality tested in test_add_items_persists_to_db()


class TestCustomSQLAlchemySessionInsertedItems:
    @pytest.mark.asyncio
    async def test_inserted_items_db_ids_match_stored_content(self, test_session):
        """
        The db_ids in inserted_items correspond to the correct rows: fetching
        by those IDs returns content identical to what was inserted.
        """
        from sqlalchemy import select

        items = [
            _msg("user", "Tell me about this book."),
            _msg("assistant", "This is a great read."),
        ]

        await test_session.add_items(items)

        async with test_session._session_factory() as sess:
            for db_id, original_item in test_session.inserted_items:
                result = await sess.execute(
                    select(test_session._messages.c.message_data).where(
                        test_session._messages.c.id == db_id
                    )
                )
                row = result.scalar_one()
                import json

                stored_item = json.loads(row)
                assert stored_item.get("content") == original_item.get("content")
                assert stored_item.get("role") == original_item.get("role")

    @pytest.mark.asyncio
    async def test_add_items_populates_inserted_items(self, test_session):
        """
        After add_items(), inserted_items contains one (db_id, item) pair per
        inserted row, each db_id is a positive integer assigned by PostgreSQL,
        items are in insertion order, and a second call accumulates rather than
        replaces.
        """
        items = [
            _msg("user", "Find climate books."),
            _msg("assistant", "I found several."),
        ]

        await test_session.add_items(items)

        assert len(test_session.inserted_items) == len(items)
        for db_id, item in test_session.inserted_items:
            assert isinstance(db_id, int)
            assert db_id > 0

        # Order: items in inserted_items must match the original insertion order.
        assert [item for _, item in test_session.inserted_items] == items

        # Accumulation: a second call extends rather than replaces inserted_items.
        second_items = [_msg("user", "Any fiction?")]
        await test_session.add_items(second_items)
        assert len(test_session.inserted_items) == len(items) + len(second_items)

    @pytest.mark.asyncio
    async def test_inserted_items_returns_defensive_copy(self, test_session):
        """Mutating the list returned by inserted_items does not affect internal state."""
        await test_session.add_items([_msg("user", "hello")])

        snapshot = test_session.inserted_items
        snapshot.clear()

        assert len(test_session.inserted_items) == 1


def _make_chat_completion(content: str) -> ChatCompletion:
    return ChatCompletion(
        id="fake-id",
        created=0,
        model="test-model",
        object="chat.completion",
        choices=[
            Choice(
                finish_reason="stop",
                index=0,
                message=ChatCompletionMessage(
                    role="assistant", content=content, tool_calls=None
                ),
            )
        ],
    )


class TestCustomSQLAlchemyNewItems:
    @pytest.mark.asyncio
    async def test_new_items_strict_subset_of_inserted_items_for_str_input(
        self, test_session
    ):
        """
        When Runner.run(input=<str>) is used, RunResult.new_items (converted to
        input item dicts) must be a strict subset of session.inserted_items.

        Context: the SDK persists Runner.run(input=) as input item(s) via
        session.add_items() but does NOT surface that input item(s) in new_items.

        Requires a live PostgreSQL database.
        """

        mock_client = AsyncMock(spec=AsyncOpenAI)
        mock_client.chat.completions.create = AsyncMock(
            # Optionally: include tool calls so the runner, loops and produces non-message items
            return_value=_make_chat_completion("Generic LLM response")
        )

        model = OpenAIChatCompletionsModel(
            model="test-model", openai_client=mock_client
        )
        agent = Agent(
            name="Test Agent",
            model=model,
            instructions="Mock instructions",
        )

        # Note: the runner creates 1 new item bc the agent responds with message
        # and no tool calls (meeting the agent loop stopping conditions)
        result = await Runner.run(
            agent,
            "Mock input user message",
            session=test_session,
            run_config=RunConfig(tracing_disabled=True),
        )

        new_items = [
            item.to_input_item()
            for item in result.new_items
            # if not isinstance(item, ToolApprovalItem) # we aren't using tool approval
        ]
        inserted_items = [item for _, item in test_session.inserted_items]

        # Every new_item (converted) must appear in inserted_items (subset)
        for item in new_items:
            assert item in inserted_items, (
                f"new_items item not found in inserted_items: {item}"
            )

        # inserted_items must be strictly larger (contains the input message too)
        assert len(inserted_items) > len(new_items)
