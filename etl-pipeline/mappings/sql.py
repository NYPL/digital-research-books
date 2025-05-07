from .base_mapping import BaseMapping
from logger import create_log

logger = create_log(__name__)


class SQLMapping(BaseMapping):
    def __init__(self, source, constants):
        super().__init__(source, constants)

    def applyMapping(self):
        newRecord = self.initEmptyRecord()

        for field, structure in self.mapping.items():
            if isinstance(structure, tuple):
                sourceValue = self.source.get(structure[0], None)
                fieldData = self.getFieldData(sourceValue, structure[1])
            else:
                fieldData = list(
                    filter(
                        None,
                        [
                            self.getFieldData(self.source.get(s[0], None), s[1])
                            for s in structure
                        ],
                    )
                )

            setattr(newRecord, field, fieldData)

        self.record = newRecord
        self.applyFormatting()

    def getFieldData(self, sourceValue, structure):
        if isinstance(sourceValue, list):
            return [self.getFieldData(s, structure) for s in sourceValue]
        elif isinstance(sourceValue, dict):
            try:
                return self.formatter.format(structure, **sourceValue)
            except KeyError:
                pass
            except IndexError:
                logger.debug(
                    f"Failed to get {sourceValue} data {structure} from due to IndexError"
                )
                raise IndexError
            except TypeError as err:
                logger.debug(
                    f"Failed to get {sourceValue} data from {structure} due to TypeError"
                )
                raise err
        elif sourceValue != None:
            try:
                return self.formatter.format(structure, sourceValue)
            except IndexError:
                pass
