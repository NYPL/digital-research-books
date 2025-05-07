class Link:
    ALLOWED_FIELDS = [
        "href",
        "type",
        "rel",
        "identifier",
        "title",
        "templated",
        "language",
        "alternate",
        "children",
        "properties",
        "height",
        "width",
        "duration",
        "bitrate",
    ]

    REQUIRED_FIELDS = ["href"]

    def __init__(self, **kwargs):
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
        for reqField in self.REQUIRED_FIELDS:
            if getattr(self, reqField, None) is None:
                raise OPDS2LinkException("{} must be present in Link".format(reqField))

        if len(self.attrs - set(self.ALLOWED_FIELDS)) > 1:
            unpermittedAttrs = self.attrs - set(self.ALLOWED_FIELDS)
            raise OPDS2LinkException(
                "{} fields are not permitted in Links".format(
                    ",".join(list(unpermittedAttrs))
                )
            )

        for field in self.ALLOWED_FIELDS:
            try:
                yield field, getattr(self, field)
            except AttributeError:
                pass

    def __repr__(self):
        return "<Link(href={}, rel={})>".format(self.href, self.rel)


class OPDS2LinkException(Exception):
    pass
