"""automatic_collections

Revision ID: e46b30dd3ff5
Revises: 9e33fa16ba82
Create Date: 2023-02-06 11:44:29.000880

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'e46b30dd3ff5'
down_revision = '9e33fa16ba82'
branch_labels = None
depends_on = None


def upgrade():
    # TODO: Maybe add some CHECK constraints around some of these fields? Or just
    # enforce in code upon creation.
    op.create_table(
        "automatic_collection_definition",
        sa.Column(
            'collection_id',
            sa.Integer,
            sa.ForeignKey('collections.id', ondelete='CASCADE'),
            primary_key=True,
        ),
        sa.Column(
            'query',
            sa.ARRAY(sa.String),
        ),
        sa.Column(
            'sort',
            sa.ARRAY(sa.String),
        ),
        sa.Column(
            'filter',
            sa.ARRAY(sa.String),
        ),
        sa.Column(
            'limit',
            sa.Integer,
            nullable=False,
        ),
    )
    collection_type = postgresql.ENUM('static', 'automatic', name="collection_type")
    collection_type.create(op.get_bind(), checkfirst=True)
    op.add_column(
        'collections',
        sa.Column(
            "type",
            collection_type,
            default="static",
        ),
    )


def downgrade():
    op.drop_column("collections", "type")
    op.drop_table("automatic_collection_definition")
    collection_type = postgresql.ENUM('static', 'automatic', name="collection_type")
    collection_type.drop(op.get_bind())
