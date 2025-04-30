class Metadata:
    def __init__(self, *args, **kwargs):
        self.attrs = set(kwargs.keys())

        for field, value in kwargs.items():
            setattr(self, field, value)

    def addField(self, field, value):
        setattr(self, field, value)
        self.attrs.add(field)

    def addFields(self, fields):
        if isinstance(fields, dict):
            for field, value in fields.items():
                self.addField(field, value)
        elif isinstance(fields, list):
            for field, value in fields:
                self.addField(field, value)

    def __iter__(self):
        if "title" not in self.attrs:
            raise OPDS2MetadataException("title field must be present in metadata")

        for attr in self.attrs:
            yield attr, getattr(self, attr)

    def __repr__(self):
        return "<Metadata(title={})>".format(getattr(self, "title", "NOT SET"))


class OPDS2MetadataException(Exception):
    pass
