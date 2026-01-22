"""add 10k grin table

Revision ID: f33cf7698658
Revises: e96fd76130c3
Create Date: 2026-01-22 13:21:37.752000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "f33cf7698658"
down_revision = "e96fd76130c3"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "grin_public_domain_10k",
        sa.Column("barcode", sa.Text, primary_key=True),
        sa.Column("record_id", sa.Integer, unique=True),
    )


def downgrade():
    op.drop_table("grin_public_domain_10k")
