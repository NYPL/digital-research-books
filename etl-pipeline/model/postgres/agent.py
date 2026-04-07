from sqlalchemy import Column, ForeignKey, Index, Integer, Text, TIMESTAMP
from sqlalchemy import String
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text

from .base import Base

# Original Schema design: https://openai.github.io/openai-agents-python/ref/extensions/memory/sqlalchemy_session/#__codelineno-0-124


class AgentSession(Base):
    __tablename__ = "agent_sessions"

    session_id = Column(String, primary_key=True, nullable=False)
    created_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at = Column(
        TIMESTAMP,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=text("CURRENT_TIMESTAMP"),
    )

    messages = relationship("AgentMessage", back_populates="session")

    __table_args__ = (
        # TODO: rename so that idx_ is at start to match convention
        Index("agent_sessions_created_at_idx", "created_at"),
    )

    def __repr__(self):
        return f"<AgentSession(session_id={self.session_id})>"


class AgentMessage(Base):
    __tablename__ = "agent_messages"

    id = Column(Integer, primary_key=True, nullable=False)
    session_id = Column(
        String,
        ForeignKey("agent_sessions.session_id", ondelete="CASCADE"),
        nullable=False,
    )
    message_data = Column(Text, nullable=False)
    created_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )

    session = relationship("AgentSession", back_populates="messages")

    __table_args__ = (
        Index("idx_agent_messages_session_time", "session_id", "created_at"),
    )

    def __repr__(self):
        return f"<AgentMessage(id={self.id}, session_id={self.session_id})>"
