"""add collections table

Revision ID: fabf5e5c3109
Revises:
Create Date: 2021-08-04 18:05:01.567042

"""

from alembic import op
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


# revision identifiers, used by Alembic.
revision = "fabf5e5c3109"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "collections",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("date_created", sa.TIMESTAMP, default=datetime.utcnow()),
        sa.Column(
            "date_modified",
            sa.TIMESTAMP,
            default=datetime.utcnow(),
            onupdate=datetime.utcnow(),
        ),
        sa.Column("uuid", UUID, index=True),
        sa.Column("title", sa.String, index=True),
        sa.Column("creator", sa.String, index=True),
    )

    op.create_table(
        "collection_editions",
        sa.Column("date_created", sa.TIMESTAMP, default=datetime.utcnow()),
        sa.Column(
            "date_modified",
            sa.TIMESTAMP,
            default=datetime.utcnow(),
            onupdate=datetime.utcnow(),
        ),
        sa.Column(
            "collection_id",
            sa.Integer,
            sa.ForeignKey("collections.id", ondelete="CASCADE"),
            index=True,
        ),
        sa.Column(
            "edition_id",
            sa.Integer,
            sa.ForeignKey("editions.id", ondelete="CASCADE"),
            index=True,
        ),
    )


def downgrade():
    op.drop_table("collection_editions")

    op.drop_table("collections")
