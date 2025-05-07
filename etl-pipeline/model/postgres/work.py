from sqlalchemy import Column, ForeignKey, Integer, Unicode, Table
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.ext.mutable import MutableList
from textwrap import shorten

from .base import Base, Core

WORK_IDENTIFIERS = Table(
    "work_identifiers",
    Base.metadata,
    Column("work_id", Integer, ForeignKey("works.id", ondelete="CASCADE")),
    Column("identifier_id", Integer, ForeignKey("identifiers.id", ondelete="CASCADE")),
)

WORK_RIGHTS = Table(
    "work_rights",
    Base.metadata,
    Column("work_id", Integer, ForeignKey("works.id", ondelete="CASCADE")),
    Column("rights_id", Integer, ForeignKey("rights.id", ondelete="CASCADE")),
)


class Work(Base, Core):
    __tablename__ = "works"
    id = Column(Integer, primary_key=True)
    uuid = Column(UUID(as_uuid=True), nullable=False, index=True)
    title = Column(Unicode)
    sub_title = Column(Unicode)
    alt_titles = Column(ARRAY(Unicode, dimensions=1))
    medium = Column(Unicode)
    series = Column(Unicode)
    series_position = Column(Unicode)
    authors = Column(MutableList.as_mutable(JSONB))
    contributors = Column(MutableList.as_mutable(JSONB))
    subjects = Column(JSONB)
    measurements = Column(JSONB)
    dates = Column(JSONB)
    languages = Column(JSONB)

    editions = relationship("Edition", backref="work", cascade="all, delete-orphan")

    identifiers = relationship(
        "Identifier", secondary=WORK_IDENTIFIERS, backref="works"
    )
    rights = relationship("Rights", secondary=WORK_RIGHTS, backref="works")

    def __repr__(self):
        title = (
            shorten(self.title, width=50, placeholder="...")
            if self.title
            else self.title
        )

        return f"<Work(title={title}, uuid={self.uuid})>"

    def __dir__(self):
        return [
            "uuid",
            "title",
            "sub_title",
            "alt_titles",
            "medium",
            "series",
            "series_position",
            "authors",
            "contributors",
            "subjects",
            "measurements",
            "dates",
            "languages",
        ]
