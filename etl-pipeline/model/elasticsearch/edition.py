from elasticsearch_dsl import Text, Keyword, Date, Integer, Nested, Object

from .base import BaseInner, PerLanguageField
from .language import Language
from .identifier import Identifier
from .agent import Agent
from .rights import Rights


class Edition(BaseInner):
    title = Object(PerLanguageField)
    sub_title = Object(PerLanguageField)
    alt_titles = Text(fields={"keyword": Keyword()})
    publication_place = Text(fields={"keyword": Keyword()})
    publication_date = Date(format="date_optional_time")
    edition = Text(fields={"keyword": Keyword()})
    edition_statement = Text(fields={"keyword": Keyword()})
    table_of_contents = Text()
    volume = Text(fields={"keyword": Keyword()})
    extent = Text()
    summary = Text()
    formats = Keyword()
    edition_id = Integer()

    agents = Nested(Agent)
    identifiers = Nested(Identifier)
    rights = Nested(Rights)
    languages = Nested(Language)

    @classmethod
    def getFields(cls):
        return [
            "id",
            "edition_id",
            "title",
            "sub_title",
            "publication_place",
            "publication_date",
            "edition",
            "edition_statement",
            "table_of_contents",
            "extent",
            "summary",
        ]

    def __dir__(self):
        return ["agents", "identifiers", "rights", "languages", "formats"]

    def cleanRels(self):
        for rel in dir(self):
            if isinstance(getattr(self, rel), set):
                setattr(self, rel, list(getattr(self, rel)))
