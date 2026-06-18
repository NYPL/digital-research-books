"""agent_messages_convert_message_data_to_jsonb

Revision ID: 83741c1bb491
Revises: f33cf7698658
Create Date: 2026-06-18 16:38:02.557009

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = "83741c1bb491"
down_revision = "f33cf7698658"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "agent_messages",
        "message_data",
        existing_type=sa.Text(),
        type_=JSONB(),
        postgresql_using="message_data::jsonb",
        nullable=False,
    )
    op.create_index(
        "idx_agent_messages_message_data_gin",
        "agent_messages",
        ["message_data"],
        postgresql_using="gin",
    )


def downgrade():
    op.drop_index(
        "idx_agent_messages_message_data_gin",
        table_name="agent_messages",
    )
    op.alter_column(
        "agent_messages",
        "message_data",
        existing_type=JSONB(),
        type_=sa.Text(),
        postgresql_using="message_data::text",
        nullable=False,
    )
