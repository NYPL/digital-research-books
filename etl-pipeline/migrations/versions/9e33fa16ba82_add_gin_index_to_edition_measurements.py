"""Add GIN index to edition measurements

Revision ID: 9e33fa16ba82
Revises: 975e451df332
Create Date: 2022-07-25 15:12:08.915745

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "9e33fa16ba82"
down_revision = "975e451df332"
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(
        "ix_editions_measurements", "editions", ["measurements"], postgresql_using="gin"
    )


def downgrade():
    op.drop_index("ix_editions_measurements")
