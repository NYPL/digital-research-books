"""Add owner to collections

Revision ID: 2f6b783fc4aa
Revises: 6c31ab8a023d
Create Date: 2021-08-16 16:08:00.237234

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "2f6b783fc4aa"
down_revision = "6c31ab8a023d"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("collections", sa.Column("owner", sa.Unicode))


def downgrade():
    op.drop_column("collections", "owner")
