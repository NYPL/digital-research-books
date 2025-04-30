"""add publisher_project_source field to records and items

Revision ID: 54e57fb2e1c6
Revises: e46b30dd3ff5
Create Date: 2024-06-26 15:04:24.092549

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "54e57fb2e1c6"
down_revision = "e46b30dd3ff5"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("records", sa.Column("publisher_project_source", sa.Unicode))
    op.add_column("items", sa.Column("publisher_project_source", sa.Unicode))


def downgrade():
    op.drop_column("records", "publisher_project_source")
    op.drop_column("items", "publisher_project_source")
