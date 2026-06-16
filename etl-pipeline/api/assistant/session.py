from __future__ import annotations

from sqlalchemy import insert, select, update
from sqlalchemy import text as sql_text
from sqlalchemy.exc import IntegrityError
from agents.extensions.memory import SQLAlchemySession
from agents.items import TResponseInputItem, ToolApprovalItem
from agents import RunResult


class CustomSQLAlchemySession(SQLAlchemySession):
    """
    Extends SQLAlchemySession to capture the PostgreSQL-assigned row IDs for
    every item persisted via .add_items() in the `inserted_items` attribute.

    Based on: https://github.com/openai/openai-agents-python/blob/main/src/agents/extensions/memory/sqlalchemy_session.py

    .inserted_items is a list of (db_id, item) pairs.
    .add_items() is called once at the end of the agent run in `save_result_to_session`
    (`agents/run_internal/session_persistence.py` line 286) → `session.add_items()`.

    NOTE: PostgreSQL only — uses INSERT ... RETURNING id, which is not supported by SQLite
    or other databases without modification.

    CustomSQLAlchemySession.inserted_items vs RunResult.new_items:
        - .new_items does not include input items from Runner.run(input=) while
          .inserted_items does.
        - .new_items includes tool_approval_item's while .inserted_items does not.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._inserted_items: list[tuple[int, TResponseInputItem]] = []

    async def add_items(self, items: list[TResponseInputItem]) -> None:
        if not items:
            return

        await self._ensure_tables()
        payload = [
            {
                "session_id": self.session_id,
                "message_data": await self._serialize_item(item),
            }
            for item in items
        ]

        async with self._session_factory() as sess:
            async with sess.begin():
                # Avoid check-then-insert races on the first write while keeping
                # the common path free of avoidable integrity exceptions.
                existing = await sess.execute(
                    select(self._sessions.c.session_id).where(
                        self._sessions.c.session_id == self.session_id
                    )
                )
                if not existing.scalar_one_or_none():
                    try:
                        async with sess.begin_nested():
                            await sess.execute(
                                insert(self._sessions).values(
                                    {"session_id": self.session_id}
                                )
                            )
                    except IntegrityError:
                        # Another concurrent writer created the parent row first.
                        pass

                # Insert messages in bulk
                result = await sess.execute(
                    insert(self._messages).returning(self._messages.c.id),
                    payload,
                )
                db_ids = [row[0] for row in result.all()]

                # Touch updated_at column
                await sess.execute(
                    update(self._sessions)
                    .where(self._sessions.c.session_id == self.session_id)
                    .values(updated_at=sql_text("CURRENT_TIMESTAMP"))
                )

        self._inserted_items.extend(zip(db_ids, items))

    @property
    def inserted_items(self) -> list[tuple[int, TResponseInputItem]]:
        """All (db_id, item) pairs written by add_items() during this session object's lifetime."""
        # NOTE: @property keeps the attr read-only/immutable
        return list(self._inserted_items)


def get_new_items_with_ids(
    run_result: RunResult,
    session: CustomSQLAlchemySession,
) -> list[dict]:
    """
    Returns items that are in both RunResult.new_items and CustomSQLAlchemySession.inserted_items (converted via .to_input_item()) , with DB `agent_messages.id` added as `db_id`.

    If there are multiple identically valued items in .inserted_items the first
    db_id for each match in .new_items is used (a pop-on-first-match pool correctly
    handles duplicate item content.) In case of duplicates, if .add_items()
    has been called outside of the run that produced RunResult, db_ids might
    not be correct.

    Note: ToolApprovalItem's are not persisted, thus are not in .inserted_items, and are not returned.

    """
    new_items = [
        item.to_input_item()
        for item in run_result.new_items
        if not isinstance(item, ToolApprovalItem)
    ]
    # calling .to_input_item() on ToolApprovalItem raises (agents/items.py).
    # And they are not persisted agents/run_internal/session_persistence.py, line 243.

    messages = []
    for db_id, inserted_item in session.inserted_items:
        try:
            idx = new_items.index(inserted_item)  # dict equality check
            new_items.pop(idx)
            messages.append({"db_id": db_id, **inserted_item})
        except ValueError:
            pass  # if `inserted_item` is not in `new_items` (.e.g. Runner.run(input=) items)
    return messages
