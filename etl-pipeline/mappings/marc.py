import re

from .base_mapping import BaseMapping
from logger import create_log

logger = create_log(__name__)


class MARCMapping(BaseMapping):
    def __init__(self, source, constants):
        super().__init__(source, constants)

    def applyMapping(self):
        newRecord = self.initEmptyRecord()

        for field, structure in self.mapping.items():
            try:
                if isinstance(structure, tuple):
                    fieldData = self.getFieldData(structure)[0]
                else:
                    fieldData = [f for s in structure for f in self.getFieldData(s)]
            except IndexError:
                logger.debug(f"Missing MARC data from field {field}")
                continue

            setattr(newRecord, field, fieldData)

        newRecord.identifiers = list(
            filter(lambda x: x[0] != "|", newRecord.identifiers)
        )

        self.record = newRecord
        self.applyFormatting()

    def getFieldData(self, structure):
        marcTag, stringFormat = structure

        marcData = self.getMARCData(marcTag)

        return [fd for fd in self.setFieldValues(marcData, stringFormat)]

    def getMARCData(self, marcTag):
        outData = []
        for fieldInstance in self.source.get_fields(marcTag):
            if fieldInstance.is_control_field():
                outData.append(getattr(fieldInstance, "data"))
            else:
                subfields = {
                    "ind1": fieldInstance.indicators[0],
                    "ind2": fieldInstance.indicators[1],
                }
                for i in range(0, len(fieldInstance.subfields), 2):
                    try:
                        subfieldKey = "s{}".format(int(fieldInstance.subfields[i]))
                    except ValueError:
                        subfieldKey = fieldInstance.subfields[i]

                    subfields[subfieldKey] = fieldInstance.subfields[i + 1]

                outData.append(subfields)

        return outData

    def setFieldValues(self, marcData, stringFormat):
        for marcField in marcData:
            if marcField is None:
                continue

            try:
                yield self.applyStringFormat(marcField, stringFormat)
            except KeyError:
                pass

    def applyStringFormat(self, marcField, stringFormat):
        if isinstance(marcField, str):
            outStr = self.formatter.format(stringFormat, marcField)
        else:
            outStr = self.formatter.format(stringFormat, **marcField)

        # Remove extra spaces from combining multiple subfields
        outNoDupeSpaces = re.sub(r"\s{2,}", " ", outStr)

        # Remove spaces at ends of strings, including extraneous punctuation
        # The negative lookbehind preserves punctuation with initialisms
        return re.sub(r"((?<![A-Z]{1}))[ .,;:]+(\||$)", r"\1\2", outNoDupeSpaces)
