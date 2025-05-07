"""automatic_collections

Revision ID: e46b30dd3ff5
Revises: 9e33fa16ba82
Create Date: 2023-02-06 11:44:29.000880

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "e46b30dd3ff5"
down_revision = "9e33fa16ba82"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "automatic_collection",
        sa.Column(
            "collection_id",
            sa.Integer,
            sa.ForeignKey("collections.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "keyword_query",
            sa.String,
        ),
        sa.Column(
            "author_query",
            sa.String,
        ),
        sa.Column(
            "title_query",
            sa.String,
        ),
        sa.Column(
            "subject_query",
            sa.String,
        ),
        sa.Column(
            "sort_field",
            sa.String,
            sa.CheckConstraint(r"sort_field IN ('uuid', 'title', 'author', 'date')"),
            server_default="uuid",
            nullable=False,
        ),
        sa.Column(
            "sort_direction",
            sa.String,
            sa.CheckConstraint(r"sort_direction IN ('ASC', 'DESC')"),
            server_default="ASC",
            nullable=False,
        ),
        sa.Column(
            "limit",
            sa.Integer,
            sa.CheckConstraint(r'"limit" IS NULL OR "limit" > 0'),
        ),
    )
    collection_type = postgresql.ENUM("static", "automatic", name="collection_type")
    collection_type.create(op.get_bind(), checkfirst=True)
    op.add_column(
        "collections",
        sa.Column(
            "type",
            collection_type,
            server_default="static",
            nullable=False,
        ),
    )


def downgrade():
    op.drop_column("collections", "type")
    op.drop_table("automatic_collection")
    collection_type = postgresql.ENUM("static", "automatic", name="collection_type")
    collection_type.drop(op.get_bind())
