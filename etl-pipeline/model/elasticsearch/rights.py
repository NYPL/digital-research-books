from elasticsearch_dsl import Text, Keyword

from .base import BaseInner


class Rights(BaseInner):
    source = Keyword()
    license = Keyword()
    rights_statement = Text(fields={"keyword": Keyword()})
    rights_reason = Text(fields={"keyword": Keyword()})

    @classmethod
    def getFields(cls):
        return ["source", "license", "rights_statement", "rights_reason"]

    def __key(self):
        return (self.source, self.license, self.rights_statement)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, Rights):
            return self.__key() == other.__key()
        return NotImplemented
