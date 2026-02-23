from sqlalchemy import DateTime, Unicode, Integer, Column, Table, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB, ARRAY, UUID
from sqlalchemy.orm import relationship

from .base import Base, Core

ITEM_IDENTIFIERS = Table(
    "item_identifiers",
    Base.metadata,
    Column("item_id", Integer, ForeignKey("items.id", ondelete="CASCADE")),
    Column("identifier_id", Integer, ForeignKey("identifiers.id", ondelete="CASCADE")),
)

ITEM_LINKS = Table(
    "item_links",
    Base.metadata,
    Column("item_id", Integer, ForeignKey("items.id", ondelete="CASCADE")),
    Column("link_id", Integer, ForeignKey("links.id", ondelete="CASCADE")),
)

ITEM_RIGHTS = Table(
    "item_rights",
    Base.metadata,
    Column("item_id", Integer, ForeignKey("items.id", ondelete="CASCADE")),
    Column("rights_id", Integer, ForeignKey("rights.id", ondelete="CASCADE")),
)


class Item(Base, Core):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True)
    source = Column(Unicode, nullable=False, index=True)
    publisher_project_source = Column(Unicode, index=True)
    content_type = Column(Unicode)
    contributors = Column(JSONB)
    modified = Column(DateTime)
    drm = Column(Unicode)
    measurements = Column(JSONB)
    physical_location = Column(JSONB)

    edition_id = Column(
        Integer, ForeignKey("editions.id", ondelete="CASCADE"), index=True
    )
    record_id = Column(Integer, ForeignKey("records.id", ondelete="CASCADE"))

    identifiers = relationship(
        "Identifier", secondary=ITEM_IDENTIFIERS, backref="items", cascade="all, delete"
    )
    links = relationship(
        "Link", secondary=ITEM_LINKS, backref="items", cascade="all, delete"
    )
    rights = relationship(
        "Rights", secondary=ITEM_RIGHTS, backref="items", cascade="all, delete"
    )

    def __repr__(self):
        return "<Item(source={}, content_type={})>".format(
            self.source, self.content_type
        )

    def __dir__(self):
        return [
            "source",
            "publisher_project_source",
            "content_type",
            "contributors",
            "modified",
            "drm",
            "measurements",
        ]
