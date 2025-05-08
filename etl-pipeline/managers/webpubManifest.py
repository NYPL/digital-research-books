import json
import os
from typing import NoReturn


class WebpubManifest:
    # TODO: change - the library simplified PDF profile no longer exists
    WEBPUB_PDF_PROFILE = "http://librarysimplified.org/terms/profiles/pdf"

    def __init__(self, source, sourceType):
        self.metadata: dict = {"@type": "https://schema.org/Book"}
        self.links: list = [{"href": source, "type": sourceType, "rel": "alternate"}]
        self.readingOrder: list = []
        self.resources: set = set()
        self.tableOfContents: list = []

        self.openSection = None

    # TODO validate kwargs against schema.org/Book
    def addMetadata(
        self, dcdwRecord: object, conformsTo: str = WEBPUB_PDF_PROFILE
    ) -> None:
        self.metadata["title"] = dcdwRecord.title

        if dcdwRecord.authors and len(dcdwRecord.authors) > 0:
            self.metadata["author"] = list(dcdwRecord.authors[0].split("|"))[0]

        isbns = list(filter(lambda x: x[-4:] == "isbn", dcdwRecord.identifiers))

        if len(isbns) > 0:
            self.metadata["identifier"] = "urn:isbn:{}".format(isbns[0][:-5])

        if conformsTo:
            self.metadata["conformsTo"] = conformsTo

    def addSection(self, sectionTitle: str, sectionURL: str) -> NoReturn:
        if self.openSection:
            self.tableOfContents.append(self.openSection)

        self.openSection = {"href": sectionURL, "title": sectionTitle, "children": []}

    def closeSection(self) -> NoReturn:
        if self.openSection:
            self.tableOfContents.append(self.openSection)

        self.openSection = None

    # TODO Make it possible to add subsections iteratively
    def addChapter(self, link: str, title: str, subsections=None) -> None:
        component = {"href": link, "title": title}
        tocEntry = component.copy()

        if subsections and isinstance(subsections, list):
            tocEntry["children"] = subsections

        if self.openSection:
            self.openSection["children"].append(tocEntry)
        else:
            self.tableOfContents.append(tocEntry)

        self.addReadingOrder(component)
        self.addResource(component["href"])

    def addReadingOrder(self, component: dict) -> NoReturn:
        component["type"] = "application/pdf"

        self.readingOrder.append(component)

    def addResource(self, href: str) -> NoReturn:
        rootHref, *_ = href.split("#")

        self.resources.add(rootHref)

    def toDict(self) -> dict:
        return {
            "context": "https://{}-s3.amazonaws.com/manifests/context.jsonld".format(
                os.environ["FILE_BUCKET"]
            ),
            "metadata": self.metadata,
            "links": self.links,
            "readingOrder": self.readingOrder,
            "resources": [
                {"href": res, "type": "application/pdf"} for res in self.resources
            ],
            "toc": self.tableOfContents,
        }

    def toJson(self) -> str:
        return json.dumps(self.toDict())
