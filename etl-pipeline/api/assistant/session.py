from sqlalchemy import Column, Table
from sqlalchemy.dialects.postgresql import JSONB
from agents.extensions.memory import SQLAlchemySession
from agents.items import TResponseInputItem


class JSONBSQLAlchemySession(SQLAlchemySession):
    """SQLAlchemySession variant that stores agent_messages.message_data as
    JSONB instead of Text.

    Overrides _serialize_item/_deserialize_item so asyncpg receives a dict
    (not a JSON string) for the JSONB column, avoiding double-encoding.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # MAYBE: just import our models from postgres/agent.py
        self._messages = Table(
            self._messages.name,
            self._metadata,
            Column("message_data", JSONB, nullable=False),
            extend_existing=True,
        )

    async def _serialize_item(self, item: TResponseInputItem) -> dict:
        return item

    async def _deserialize_item(self, item: dict) -> TResponseInputItem:
        return item
