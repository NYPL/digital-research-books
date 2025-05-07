class Navigation:
    def __init__(self, **kwargs):
        self.href = kwargs.get("href", None)
        self.title = kwargs.get("title", None)
        self.rel = kwargs.get("rel", None)
        self.type = kwargs.get("type", None)

    def addField(self, field, value):
        setattr(self, field, value)

    def addFields(self, fields):
        if isinstance(fields, dict):
            for field, value in fields.items():
                self.addField(field, value)
        elif isinstance(fields, list):
            for field, value in fields:
                self.addField(field, value)

    def __dir__(self):
        return ["href", "title", "rel", "type"]

    def __iter__(self):
        if self.title is None:
            raise OPDS2NavigationException("title field must be present in metadata")

        for attr in dir(self):
            yield attr, getattr(self, attr)

    def __repr__(self):
        return "<Navigation(title={}, type={}, href={})>".format(
            getattr(self, "title", "NOT SET"),
            getattr(self, "type", "NOT SET"),
            getattr(self, "href", "NOT SET"),
        )


class OPDS2NavigationException(Exception):
    pass
