from sqlalchemy import Unicode, Integer, Column, UniqueConstraint

from .base import Base, Core


class Identifier(Base, Core):
    __tablename__ = "identifiers"
    id = Column(Integer, primary_key=True)
    identifier = Column(Unicode, nullable=False, index=True)
    authority = Column(Unicode, nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint("identifier", "authority", name="uc_identifier_authority"),
    )

    def __repr__(self):
        return "<Identifier(value={}, authority={})>".format(
            self.identifier, self.authority
        )

    def __dir__(self):
        return ["identifier", "authority"]
