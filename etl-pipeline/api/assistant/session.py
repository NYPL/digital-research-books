from __future__ import annotations

from sqlalchemy import insert, select, update
from sqlalchemy import text as sql_text

from agents.extensions.memory import SQLAlchemySession
from agents.items import TResponseInputItem


class CustomSQLAlchemySession(SQLAlchemySession):
    """
    Extends SQLAlchemySession to capture the PostgreSQL-assigned row IDs for
    every item persisted via .add_items() in the `inserted_items` attribute.

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

        serialized = [await self._serialize_item(item) for item in items]
        payload = [
            {"session_id": self.session_id, "message_data": s} for s in serialized
        ]

        async with self._session_factory() as sess:
            async with sess.begin():
                existing = await sess.execute(
                    select(self._sessions.c.session_id).where(
                        self._sessions.c.session_id == self.session_id
                    )
                )
                if not existing.scalar_one_or_none():
                    await sess.execute(
                        insert(self._sessions).values({"session_id": self.session_id})
                    )

                result = await sess.execute(
                    insert(self._messages).returning(self._messages.c.id),
                    payload,
                )
                db_ids = [row[0] for row in result.all()]

                await sess.execute(
                    update(self._sessions)
                    .where(self._sessions.c.session_id == self.session_id)
                    .values(updated_at=sql_text("CURRENT_TIMESTAMP"))
                )

        self._inserted_items.extend(zip(db_ids, items))

    @property
    def inserted_items(self) -> list[tuple[int, TResponseInputItem]]:
        """All (db_id, item) pairs written by add_items() during this session object's lifetime."""
        return list(self._inserted_items)
