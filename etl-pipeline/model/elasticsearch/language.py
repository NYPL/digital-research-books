from elasticsearch_dsl import Keyword

from .base import BaseInner


class Language(BaseInner):
    language = Keyword()
    iso_2 = Keyword()
    iso_3 = Keyword()

    @classmethod
    def getFields(cls):
        return ["language", "iso_2", "iso_3"]

    def __key(self):
        return self.iso_3

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, Language):
            return self.__key() == other.__key()
        return NotImplemented
