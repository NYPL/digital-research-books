"""add description to collections

Revision ID: 6c31ab8a023d
Revises: fabf5e5c3109
Create Date: 2021-08-09 18:03:18.749951

"""

from alembic import op
from datetime import datetime
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6c31ab8a023d"
down_revision = "fabf5e5c3109"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("collections", sa.Column("description", sa.Unicode))

    op.drop_column("collection_editions", "date_created")
    op.drop_column("collection_editions", "date_modified")


def downgrade():
    op.drop_column("collections", "description")

    op.add_column(
        "collection_editions",
        sa.Column("date_created", sa.TIMESTAMP, default=datetime.utcnow()),
    )
    op.add_column(
        "collection_editions",
        sa.Column(
            "date_modified",
            sa.TIMESTAMP,
            default=datetime.utcnow(),
            onupdate=datetime.utcnow(),
        ),
    )
