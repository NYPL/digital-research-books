from collections import defaultdict
import os

from model import Edition
from .metadata import Metadata
from .link import Link
from .image import Image
from ..utils import APIUtils


class Publication:
    METADATA_FIELDS = [
        "identifier",
        "title",
        "subtitle",
        "sortAs",
        "author",
        "translator",
        "editor",
        "illustrator",
        "artist",
        "colorist",
        "inker",
        "penciler",
        "letterer",
        "narrator",
        "contributor",
        "name",
        "language",
        "subject",
        "numberOfPages",
        "duration",
        "abridged",
        "publisher",
        "imprint",
        "modified",
        "published",
        "description",
        "belongsTo",
        "series",
        "collection",
        "position",
        "alternate",
        "isbn",
        "locationCreated",
    ]

    def __init__(self, metadata={}, links=[], images=[]):
        metadata["@type"] = "http://schema.org/Book"

        self.metadata = Metadata(**metadata)
        self.links = [Link(**link) for link in links]
        self.images = [Image(**image) for image in images]
        self.editions = []
        self.type = "application/opds-publication+json"

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

    def addImages(self, images):
        for image in images:
            self.addImage(image)

    def addImage(self, image):
        if isinstance(image, dict):
            image = Image(**image)

        self.images.append(image)

    def addEditions(self, editions):
        for edition in editions:
            self.addEdition(edition)

    def addEdition(self, edition):
        if isinstance(edition, dict):
            edition = Publication(metadata=edition["metadata"], links=edition["links"])

        self.editions.append(edition)

    def parseWorkToPublication(self, workRecord, searchResult=True):
        # Title Fields
        self.metadata.addField("title", workRecord.title)
        self.metadata.addField("sortAs", workRecord.title.lower())
        self.metadata.addField("subtitle", workRecord.sub_title)
        self.metadata.addField("alternate", workRecord.alt_titles)

        # Identifier
        self.setBestIdentifier(workRecord.identifiers)

        # Authors/Contributors
        self.metadata.addField(
            "author", ", ".join([a["name"] for a in workRecord.authors])
        )
        self.setContributors(workRecord.contributors)

        # Languages
        self.metadata.addField(
            "language",
            ",".join(
                lang.get("iso_3", "")
                for lang in list(filter(None, workRecord.languages))
            ),
        )

        # Created/Modified
        self.metadata.addField("created", workRecord.date_created)
        self.metadata.addField("modified", workRecord.date_modified)

        # Subjects
        self.metadata.addField(
            "subject", ", ".join(s["heading"] for s in workRecord.subjects)
        )

        # Links
        self.addLink(
            {
                "href": "/opds/publication/{}".format(workRecord.uuid),
                "rel": "self",
                "type": "application/opds-publication+json",
            }
        )
        self.setPreferredLink(workRecord.editions)

        # Covers
        self.findAndAddCover(workRecord)

        # If not in search context, add editions block
        if searchResult is False:
            self.parseEditions(workRecord.editions)

    def parseEditionToPublication(self, editionRecord):
        # Title Fields
        self.metadata.addField("title", editionRecord.title)
        self.metadata.addField("sortAs", editionRecord.title.lower())
        self.metadata.addField("subtitle", editionRecord.sub_title)
        self.metadata.addField("alternate", editionRecord.alt_titles)

        # Creator (from work record)
        try:
            self.metadata.addField("creator", editionRecord.work.authors[0]["name"])
        except IndexError:
            pass

        # Identifier
        self.setBestIdentifier(editionRecord.identifiers)

        # Publishers/Contributors/Authors
        self.metadata.addField(
            "publisher", ", ".join([p["name"] for p in editionRecord.publishers])
        )
        self.setContributors(editionRecord.contributors)

        # Publication Fields
        if editionRecord.publication_date:
            publicationYear = editionRecord.publication_date.year
        else:
            publicationYear = ""
        self.metadata.addField("published", publicationYear)

        self.metadata.addField("locationCreated", editionRecord.publication_place)

        # Summary
        self.metadata.addField("description", editionRecord.summary)

        # Languages
        self.metadata.addField(
            "language",
            ",".join(
                lang.get("iso_3", "")
                for lang in list(filter(None, editionRecord.languages))
            ),
        )

        # Created/Modified
        self.metadata.addField("created", editionRecord.date_created)
        self.metadata.addField("modified", editionRecord.date_modified)

        # Covers
        self.findAndAddCover(editionRecord)

        host = (
            "digital-research-books-beta"
            if os.environ["ENVIRONMENT"] == "production"
            else "drb-qa"
        )

        identifier = ""

        # Acquisition Links
        for item in editionRecord.items:
            for link in item.links:
                # Read Online Link
                if "reader" in link.flags:
                    if link.flags["reader"] == True:
                        self.addLink(
                            {
                                "href": "https://{}.nypl.org/read/{}".format(
                                    host, link.id
                                ),
                                "type": link.media_type,
                                "rel": "http://opds-spec.org/acquisition/open-access",
                                "identifier": "readable",
                            }
                        )
                    # Non-readable links
                    else:
                        identifier = self.getLinkIdentifier(link)
                        self.addLink(
                            {
                                "href": link.url,
                                "type": link.media_type,
                                "rel": "http://opds-spec.org/acquisition/open-access",
                                "identifier": identifier,
                            }
                        )

                # Non-readable links
                else:
                    identifier = self.getLinkIdentifier(link)
                    self.addLink(
                        {
                            "href": link.url,
                            "type": link.media_type,
                            "rel": "http://opds-spec.org/acquisition/open-access",
                            "identifier": identifier,
                        }
                    )
            for rights in item.rights:
                self.metadata.addField(
                    "rights",
                    {
                        "source": rights.source,
                        "license": rights.license,
                        "rightsStatement": rights.rights_statement,
                    },
                )

    def parseEditions(self, editions):
        for edition in editions:
            editionPub = Publication()
            editionPub.parseEditionToPublication(edition)
            self.addEdition(editionPub)

    def setBestIdentifier(self, identifiers):
        for idType in ["isbn", "issn", "oclc", "lccn", "owi"]:
            typeIDs = list(filter(lambda x: x.authority == idType, identifiers))

            if len(typeIDs) > 0:
                self.metadata.addField(
                    "identifier", "urn:{}:{}".format(idType, typeIDs[0].identifier)
                )
                break

    def setContributors(self, contributors):
        contributorsByRole = defaultdict(list)

        for contrib in contributors:
            for role in contrib["roles"]:
                contributorsByRole[role.lower()].append(contrib["name"])

        for role, names in contributorsByRole.items():
            if role in self.METADATA_FIELDS:
                self.metadata.addField(role, ", ".join(names))

    def setPreferredLink(self, editions):
        for ed in editions:
            if len(ed.items) > 0:
                firstLink = ed.items[0].links[0]
                break

        self.addLink(
            {
                "href": firstLink.url,
                "type": firstLink.media_type,
                "rel": "http://opds-spec.org/acquisition/open-access",
            }
        )

    def getLinkIdentifier(self, link):
        """#Setting identifier value based on link media type when fetching collections"""

        linkIdent = ""
        if link.media_type in APIUtils.FORMAT_CROSSWALK["readable"]:
            linkIdent = "readable"
        elif link.media_type in APIUtils.FORMAT_CROSSWALK["downloadable"]:
            linkIdent = "downloadable"
        elif link.media_type in APIUtils.FORMAT_CROSSWALK["requestable"]:
            linkIdent = "requestable"
        elif link.media_type in APIUtils.FORMAT_CROSSWALK["html_catalog"]:
            linkIdent = "catalog"
        else:
            linkIdent = "other"
        return linkIdent

    def findAndAddCover(self, record):
        if isinstance(record, Edition):
            editions = [record]
        else:
            editions = [e for e in record.editions]

        for edition in editions:
            for link in edition.links:
                if link.media_type in ["image/jpeg", "image/png"]:
                    self.addImage({"href": link.url, "type": link.media_type})
                    break

            if len(self.images) > 0:
                return

        # Add default cover image if none found
        self.addImage({"href": os.environ["DEFAULT_COVER_URL"], "type": "image/png"})

    def __dir__(self):
        return ["type", "metadata", "links", "editions", "images"]

    def __iter__(self):
        if len(self.images) == 0:
            raise OPDS2PublicationException(
                "At least one image must be present in an OPDS2 publication"
            )

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
        return "<Publication(title={}, author={})>".format(
            self.metadata.title, self.metadata.author
        )


class OPDS2PublicationException(Exception):
    pass
