"""Add grin_status table

Revision ID: e96fd76130c3
Revises: eb6ce7780436
Create Date: 2025-05-01 16:26:35.530327

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e96fd76130c3"
down_revision = "eb6ce7780436"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "grin_statuses",
        sa.Column("barcode", sa.Unicode(), nullable=False),
        sa.Column("record_id", sa.Integer(), nullable=True),
        sa.Column("failed_download", sa.INTEGER(), nullable=True),
        sa.Column("state", sa.Unicode(), nullable=False),
        sa.Column("date_created", sa.DateTime(), nullable=True),
        sa.Column("date_modified", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(
            ["record_id"],
            ["records.id"],
        ),
        sa.PrimaryKeyConstraint("barcode"),
        sa.UniqueConstraint("record_id"),
    )
    op.create_index(
        op.f("ix_grin_statuses_date_created"),
        "grin_statuses",
        ["date_created"],
        unique=False,
    )
    op.create_index(
        op.f("ix_grin_statuses_date_modified"),
        "grin_statuses",
        ["date_modified"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_grin_statuses_date_modified"), table_name="grin_statuses")
    op.drop_index(op.f("ix_grin_statuses_date_created"), table_name="grin_statuses")
    op.drop_table("grin_statuses")
    # ### end Alembic commands ###
