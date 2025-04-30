from .base_mapping import BaseMapping


class JSONMapping(BaseMapping):
    def __init__(self, source, constants):
        super().__init__(source, constants)

    def applyMapping(self):
        self.record = self.initEmptyRecord()

        for field, structure in self.mapping.items():
            if isinstance(structure, list):
                formattedValue = list(
                    filter(None, [self.formatString(s) for s in structure])
                )
            else:
                formattedValue = self.formatString(structure)

            setattr(self.record, field, formattedValue)

        self.applyFormatting()

    # TODO Implement means of parsing nested JSON objects
    def formatString(self, structure):
        if isinstance(structure[0], list):
            values = [self.source.get(s, None) for s in structure[0]]
        else:
            values = [self.source.get(structure[0], None)]

        cleanValues = list(filter(lambda x: x not in [[], {}, None, ""], values))

        if len(cleanValues) > 0 and all([isinstance(v, list) for v in cleanValues]):
            return [
                structure[1].format(*s)
                if isinstance(s, list)
                else structure[1].format(s)
                for v in cleanValues
                for s in v
            ]
        elif len(cleanValues) > 0:
            return structure[1].format(*cleanValues)

        return None
