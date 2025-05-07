"""add user table for basic auth

Revision ID: 975e451df332
Revises: 2f6b783fc4aa
Create Date: 2021-08-18 15:36:33.313176

"""

from alembic import op
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import BYTEA


# revision identifiers, used by Alembic.
revision = "975e451df332"
down_revision = "2f6b783fc4aa"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "users",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("date_created", sa.TIMESTAMP, default=datetime.utcnow()),
        sa.Column(
            "date_modified",
            sa.TIMESTAMP,
            default=datetime.utcnow(),
            onupdate=datetime.utcnow(),
        ),
        sa.Column("user", sa.String, index=True, unique=True),
        sa.Column("password", BYTEA),
        sa.Column("salt", BYTEA),
    )


def downgrade():
    op.drop_table("users")
