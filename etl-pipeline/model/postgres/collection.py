import enum

from sqlalchemy import (
    Column,
    Enum,
    ForeignKey,
    Integer,
    String,
    Table,
    Unicode,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from .base import Base, Core

COLLECTION_EDITIONS = Table(
    "collection_editions",
    Base.metadata,
    Column("collection_id", Integer, ForeignKey("collections.id", ondelete="CASCADE")),
    Column("edition_id", Integer, ForeignKey("editions.id", ondelete="CASCADE")),
)


class CollectionType(str, enum.Enum):
    static = "static"
    automatic = "automatic"


class AutomaticCollection(Base):
    __tablename__ = "automatic_collection"

    collection_id = Column(
        "collection_id",
        Integer,
        ForeignKey("collections.id", ondelete="CASCADE"),
        primary_key=True,
    )

    keyword_query = Column("keyword_query", String)
    author_query = Column("author_query", String)
    title_query = Column("title_query", String)
    subject_query = Column("subject_query", String)

    sort_field = Column("sort_field", String, nullable=False, default="uuid")
    sort_direction = Column("sort_direction", String, nullable=False, default="ASC")

    limit = Column("limit", Integer)


class Collection(Base, Core):
    __tablename__ = "collections"

    id = Column(Integer, primary_key=True)
    uuid = Column(UUID(as_uuid=True), index=True)
    title = Column(Unicode, index=True)
    creator = Column(Unicode, index=True)
    description = Column(Unicode)
    owner = Column(Unicode)
    type = Column(Enum(CollectionType))

    editions = relationship(
        "Edition",
        secondary=COLLECTION_EDITIONS,
        backref="collections",
        cascade="all, delete",
    )

    auto = relationship("AutomaticCollection", backref="collections")

    def __repr__(self):
        return "<Collection(uuid={}, title={}, creator={}, items={})>".format(
            self.uuid, self.title, self.creator, len(self.editions)
        )

    def __dir__(self):
        return ["uuid", "title", "creator"]
