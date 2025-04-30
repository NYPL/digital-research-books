"""Adds record id to items

Revision ID: 0cebb9a98a6f
Revises: cc966d5a6ca0
Create Date: 2024-12-23 12:26:06.585604

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0cebb9a98a6f"
down_revision = "cc966d5a6ca0"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("items", sa.Column("record_id", sa.Integer))


def downgrade():
    op.drop_column("items", "record_id")
