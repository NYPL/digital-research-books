"""add unique constraint to work_identifiers table

Revision ID: cc966d5a6ca0
Revises: 54e57fb2e1c6
Create Date: 2024-09-05 16:48:16.569654

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "cc966d5a6ca0"
down_revision = "54e57fb2e1c6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint(
        "unique_work_identifier", "work_identifiers", ["work_id", "identifier_id"]
    )


def downgrade():
    op.drop_constraint("unique_work_identifier", "work_identifiers", type_="unique")
