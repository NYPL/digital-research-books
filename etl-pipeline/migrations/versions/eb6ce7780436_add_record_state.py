"""Add record state

Revision ID: eb6ce7780436
Revises: 0cebb9a98a6f
Create Date: 2025-04-24 11:09:19.838900

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ENUM


# revision identifiers, used by Alembic.
revision = 'eb6ce7780436'
down_revision = '0cebb9a98a6f'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('records', sa.Column('state', sa.String, nullable=True))


def downgrade():
    op.drop_column('records', 'state')
