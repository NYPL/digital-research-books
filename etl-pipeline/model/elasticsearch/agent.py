from elasticsearch_dsl import Text, Keyword

from .base import BaseInner, plain_ascii


class Agent(BaseInner):
    name = Text(analyzer=plain_ascii, fields={"keyword": Keyword()})
    sort_name = Keyword(index=False)
    lcnaf = Keyword()
    viaf = Keyword()
    roles = Keyword()

    @classmethod
    def getFields(cls):
        return ["name", "sort_name", "lcnaf", "viaf", "biography"]

    def __key(self):
        return (self.name, self.lcnaf, self.viaf)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, Agent):
            return self.__key() == other.__key()
        return NotImplemented
