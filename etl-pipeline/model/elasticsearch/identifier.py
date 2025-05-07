from elasticsearch_dsl import Keyword

from .base import BaseInner


class Identifier(BaseInner):
    authority = Keyword()
    identifier = Keyword()

    def __key(self):
        return (self.id_type, self.identifier)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, Identifier):
            return self.__key() == other.__key()
        return NotImplemented
