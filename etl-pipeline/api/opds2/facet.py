from .metadata import Metadata
from .link import Link


class Facet:
    def __init__(self, metadata={}, links=[]):
        self.metadata = Metadata(**metadata)
        self.links = [Link(**link) for link in links]

    def addMetadata(self, metadataFields):
        for field, value in metadataFields.items():
            setattr(self.metadata, field, value)

    def addLinks(self, links):
        for link in links:
            self.addLink(link)

    def addLink(self, link):
        if isinstance(link, dict):
            link = Link(**link)

        self.links.append(link)

    def __dir__(self):
        return ["metadata", "links"]

    def __iter__(self):
        for attr in dir(self):
            component = getattr(self, attr)

            if component is None:
                continue
            if isinstance(component, list):
                yield attr, [dict(c) for c in component]
            elif isinstance(component, Metadata):
                yield attr, dict(component)
            else:
                yield attr, component

    def __repr__(self):
        return "<Facet(title={}, links={})>".format(
            self.metadata.title, len(self.links)
        )


class OPDS2PublicationException(Exception):
    pass
