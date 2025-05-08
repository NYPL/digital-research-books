from itertools import zip_longest
from lxml import etree

from .base_mapping import BaseMapping
from logger import create_log

logger = create_log(__name__)


class XMLMapping(BaseMapping):
    def __init__(self, source, namespace, constants):
        super().__init__(source, constants)
        self.namespace = namespace

    def applyMapping(self):
        newRecord = self.initEmptyRecord()

        for field, structure in self.mapping.items():
            try:
                if isinstance(structure, tuple):
                    fieldData = self.getFieldData(structure)[0]
                else:
                    fieldData = [f for s in structure for f in self.getFieldData(s)]
            except IndexError:
                logger.debug(f"Missing data from field: {field}")
                continue

            setattr(newRecord, field, fieldData)

        newRecord.identifiers = list(
            filter(lambda x: x[0] != "|", newRecord.identifiers)
        )

        self.record = newRecord
        self.applyFormatting()

    def getFieldData(self, structure):
        if isinstance(structure[0], str):
            return [
                self.formatter.format(structure[1], e)
                for e in self.source.xpath(structure[0], namespaces=self.namespace)
            ]
        else:
            groups = []

            for component in structure[0]:
                components = []

                elements = self.source.xpath(component, namespaces=self.namespace)
                if isinstance(elements, str):
                    components.append(elements)
                elif isinstance(elements, list):
                    for elem in elements:
                        if etree.iselement(elem):
                            components.append(etree.QName(elem).localname)
                        else:
                            components.append(elem)
                else:
                    components.append("")
                groups.append(components)

            fields = list(
                filter(self.filterEmptyFields, list(zip_longest(*groups, fillvalue="")))
            )
            return [self.formatter.format(structure[1], *f) for f in fields]

    def filterEmptyFields(self, fields):
        if len(list(filter(lambda x: x != "", fields))) > 0:
            return True
        return False
