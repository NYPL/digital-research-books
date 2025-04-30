import re
from sqlalchemy import Unicode, Integer, Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import validates

from .base import Base, Core


class Link(Base, Core):
    __tablename__ = "links"
    id = Column(Integer, primary_key=True)
    url = Column(Unicode, nullable=False, index=True)
    media_type = Column(Unicode, nullable=False, index=True)
    content = Column(Unicode)
    md5 = Column(Unicode)
    flags = Column(JSONB)

    def __repr__(self):
        return "<Link(url={}, mediaType={})>".format(self.url, self.media_type)

    def __dir__(self):
        return ["url", "media_type", "content", "md5", "flags"]

    @validates("url")
    def removeHTTP(self, key, url):
        """Ensures that http:// and https:// are removed from all URLs to ensure
        equality are valid
        Arguments:
            key {str} -- Field being validated
            name {str} -- The sort_name value for the current record
        Returns:
            str -- The clean version of the URL
        """
        return Link.httpRegexSub(url)

    @staticmethod
    def httpRegexSub(url):
        if isinstance(url, str):
            return re.sub(r"^http(?:s)?:\/\/", "", url)

        return url
