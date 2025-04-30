from elasticsearch_dsl import Text, Keyword, Boolean, Nested, Object
import os

from .base import BaseDoc, PerLanguageField
from .edition import Edition
from .language import Language
from .identifier import Identifier
from .agent import Agent
from .subject import Subject


class Work(BaseDoc):
    title = Object(PerLanguageField)
    alt_titles = Object(PerLanguageField)
    sort_title = Keyword(index=False)
    uuid = Keyword(store=True)
    medium = Text(fields={"keyword": Keyword()})
    series = Text(fields={"keyword": Keyword()})
    series_position = Keyword()
    is_government_document = Boolean(multi=False)

    editions = Nested(Edition)
    identifiers = Nested(Identifier)
    subjects = Nested(Subject)
    agents = Nested(Agent)
    languages = Nested(Language)

    @classmethod
    def getFields(cls):
        return [
            "uuid",
            "title",
            "sort_title",
            "sub_title",
            "medium",
            "series",
            "series_position",
            "date_modified",
            "date_updated",
        ]

    def __dir__(self):
        return ["identifiers", "subjects", "agents", "languages"]

    def __repr__(self):
        return "<ESWork(title={}, uuid={})>".format(self.title, self.uuid)

    class Index:
        name = os.environ.get("ELASTICSEARCH_INDEX", None)
