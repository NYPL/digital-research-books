from sqlalchemy import Unicode, Integer, Column
from sqlalchemy.dialects.postgresql import BYTEA

from .base import Base, Core


class User(Base, Core):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    user = Column(Unicode, index=True, unique=True)
    password = Column(BYTEA)
    salt = Column(BYTEA)

    def __repr__(self):
        return "<User(user={})>".format(self.user)
