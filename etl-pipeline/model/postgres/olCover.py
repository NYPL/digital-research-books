from sqlalchemy import Unicode, Integer, Column, UniqueConstraint

from .base import Base, Core


class OpenLibraryCover(Base, Core):
    __tablename__ = "ol_covers"
    id = Column(Integer, primary_key=True)
    value = Column(Unicode, nullable=False, index=True)
    name = Column(Unicode, nullable=False, index=True)
    olid = Column(Unicode, nullable=False)
    cover_id = Column(Unicode, nullable=False)

    def __repr__(self):
        return "<OLCover(value={}, name={}, olid={})>".format(
            self.value, self.name, self.olid
        )

    def __dir__(self):
        return ["value", "name", "olid", "cover_id"]
