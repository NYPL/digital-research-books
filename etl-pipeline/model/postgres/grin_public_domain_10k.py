# NOTE: This table only exists in the production database. An alembic migration
# exists for it also: migrations/versions/f33cf7698658_add_10k_grin_table.py
from sqlalchemy import Integer, Text, Column

from .base import Base


class GrinPublicDomain10k(Base):
    __tablename__ = "grin_public_domain_10k"

    barcode = Column(Text, primary_key=True)
    record_id = Column(
        Integer, unique=True, nullable=True
    )  # NOTE: it would be better if this was not nullable
