from sqlalchemy import Unicode, Integer, Column, Date

from .base import Base, Core


class Rights(Base, Core):
    __tablename__ = "rights"
    id = Column(Integer, primary_key=True)
    source = Column(Unicode, nullable=False)
    license = Column(Unicode, nullable=False, index=True)
    rights_statement = Column(Unicode)
    rights_reason = Column(Unicode)
    rights_date = Column(Date)

    def __repr__(self):
        return "<Rights(license={}, source={})>".format(self.license, self.source)

    def __dir__(self):
        return ["source", "license", "rights_statement", "rights_reason"]
