from .group import Group
from .metadata import Metadata
from .navigation import Navigation
from .publication import Publication
from .link import Link


class Feed:
    COMPONENT_MAPPING = {
        "metadata": Metadata,
        "navigation": Navigation,
        "links": Link,
        "groups": Group,
        "publications": Publication,
    }

    def __init__(self, *args, **kwargs):
        self.metadata = kwargs.get("metadata", None)
        self.navigation = kwargs.get("navigation", None)
        self.links = kwargs.get("links", [])
        self.publications = kwargs.get("publications", [])
        self.groups = kwargs.get("groups", [])
        self.images = kwargs.get("images", [])
        self.facets = kwargs.get("facets", [])

    def addMetadata(self, metadata):
        self.metadata = self.componentizeObject("metadata", metadata)

    def addNavigation(self, navigation):
        self.addComponent("navigation", navigation)

    def addNavigations(self, navigations):
        for nav in navigations:
            self.addNavigation(nav)

    def addLink(self, link):
        self.addComponent("links", link)

    def addLinks(self, links):
        for link in links:
            self.addLink(link)

    def addPublication(self, publication):
        self.addComponent("publications", publication)

    def addPublications(self, publications):
        for pub in publications:
            self.addPublication(pub)

    def addGroup(self, group):
        self.addComponent("groups", group)

    def addGroups(self, groups):
        for group in groups:
            self.addGroup(group)

    def addImage(self, image):
        self.addComponent("images", image)

    def addImages(self, images):
        for image in images:
            self.addImage(image)

    def addFacet(self, facet):
        self.addComponent("facets", facet)

    def addFacets(self, facets):
        for facet in facets:
            self.addFacet(facet)

    def addComponent(self, name, component):
        element = getattr(self, name)

        component = self.componentizeObject(name, component)

        if element is None:
            setattr(self, name, [component])
        else:
            element.append(component)

    @classmethod
    def componentizeObject(cls, name, component):
        if isinstance(component, dict):
            ComponentClass = cls.COMPONENT_MAPPING[name]
            component = ComponentClass(**component)

        return component

    def __dir__(self):
        return [
            "metadata",
            "navigation",
            "links",
            "publications",
            "groups",
            "images",
            "facets",
        ]

    def __iter__(self):
        if (
            len(list(filter(lambda x: x.rel == "self" or "self" in x.rel, self.links)))
            == 0
        ):
            raise OPDS2FeedException('Link with rel of "self" must be present')

        for componentName in dir(self):
            component = getattr(self, componentName)

            if component is None or component == []:
                continue
            if isinstance(component, list):
                yield componentName, [dict(c) for c in component]
            else:
                yield componentName, dict(component)


class OPDS2FeedException(Exception):
    pass
