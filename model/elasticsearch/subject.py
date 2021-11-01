from elasticsearch_dsl import Keyword, Object

from .base import BaseInner, PerLanguageField


class Subject(BaseInner):
    authority = Keyword()
    control_number = Keyword()
    subject = Object(PerLanguageField)

    @classmethod
    def getFields(cls):
        return ['uri', 'authority', 'subject']