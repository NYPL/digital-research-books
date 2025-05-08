from .metadata import Metadata
from .navigation import Navigation
from .publication import Publication


class Group:
    def __init__(self, metadata={}, navigation=[], publications=[]):
        self.metadata = Metadata(**metadata)
        self.navigation = [Navigation(**n) for n in navigation]
        self.publications = [Publication(**p) for p in publications]

    def addMetadata(self, metadataFields):
        for field, value in metadataFields.items():
            setattr(self.metadata, field, value)

    def addPublication(self, publication):
        if isinstance(publication, dict):
            publication = Publication(**publication)

        self.publications.append(publication)

    def addPublications(self, publications):
        for pub in publications:
            self.addPublication(pub)

    def addNavigation(self, nav):
        if isinstance(nav, dict):
            nav = Navigation(**nav)

        self.navigation.append(nav)

    def addNavigations(self, navs):
        for nav in navs:
            self.addNavigation(nav)

    def __repr__(self):
        return "<Group(title={}, publications={}, navigation={})>".format(
            self.metadata.title, len(self.publications), len(self.navigation)
        )

    def __iter__(self):
        if len(self.publications) > 0 and len(self.navigation) > 0:
            raise OPDS2GroupException(
                "Group cannot contain publication and navigation arrays"
            )

        for attr in ["metadata", "publications", "navigation"]:
            value = getattr(self, attr)

            if isinstance(value, list):
                if len(value) > 0:
                    yield attr, [dict(item) for item in value]
            else:
                yield attr, dict(value)


class OPDS2GroupException(Exception):
    pass
