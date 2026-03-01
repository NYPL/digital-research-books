import os
import re

from digital_assets import get_stored_file_url
from .csv import CSVMapping
from model import FileFlags, Part, Source


class HathiMapping(CSVMapping):
    def __init__(self, source, constants):
        super(HathiMapping, self).__init__(source, constants)
        self.mapping = self.createMapping()

    def createMapping(self):
        return {
            "identifiers": [
                ("{}|hathi", 0),
                ("{}|hathi", 3),
                ("{}({})|generic", 6, 5),
                ("{}|oclc", 7),
                ("{}|isbn", 8),
                ("{}|issn", 9),
                ("{}|lccn", 10),
            ],
            "rights": ("hathitrust|{}|{}||{}", 2, 13, 14),
            "is_part_of": [("{}|volume", 4)],
            "title": ("{}", 11),
            "dates": [("{}|publication_date", 12), ("{}|copyright_date", 16)],
            "requires": [("{}|government_document", 15)],
            "spatial": ("{}", 17),
            "languages": [("||{}", 18)],
            "contributors": [
                ("{}|||provider", 21),
                ("{}|||responsible", 22),
                ("{}|||digitizer", 23),
            ],
            "authors": [("{}|||true", 25)],
        }

    def applyFormatting(self):
        # Set source metadata
        self.record.source = "hathitrust"
        self.record.source_id = self.record.identifiers[0]

        # Split any identifier that contains a comma into multiple values
        cleanIdentifiers = []
        for iden in self.record.identifiers:
            if "," in iden:
                sourceIdens, idenType = iden.split("|")
                idenList = sourceIdens.split(",")
                cleanIdentifiers.extend(["{}|{}".format(i, idenType) for i in idenList])
            else:
                cleanIdentifiers.append(iden)
        self.record.identifiers = cleanIdentifiers

        # Parse publisher from publication date
        self.record.dates = self.record.dates or [""]
        pubDate = self.record.dates[0]
        try:
            pubDateExtract = re.search(r"[0-9]{4}", pubDate).group(0)
            self.record.dates[0] = "{}|publication_date".format(pubDateExtract)
        except AttributeError:
            self.record.dates.pop(0)
            pubDateExtract = ""
        publisher = pubDate.replace(pubDateExtract, "").split("|")[0].strip("[], .;")
        self.record.publisher = ["{}||".format(publisher)]

        # Parse contributers into full names
        self.record.contributors = self.record.contributors or []
        for i, contributor in enumerate(self.record.contributors):
            contribElements = contributor.lower().split("|")
            fullContribName = self.constants["hathitrust"]["sourceCodes"].get(
                contribElements[0], contribElements[0]
            )
            self.record.contributors[i] = contributor.replace(
                contribElements[0], fullContribName
            )

        # Parse rights codes
        rightsElements = (
            self.record.rights.split("|") if self.record.rights else [""] * 5
        )
        rightsMetadata = self.constants["hathitrust"]["rightsValues"].get(
            rightsElements[1], {"license": "und", "statement": "und"}
        )
        self.record.rights = "hathitrust|{}|{}|{}|{}".format(
            rightsMetadata["license"],
            self.constants["hathitrust"]["rightsReasons"].get(
                rightsElements[2], rightsElements[2]
            ),
            rightsMetadata["statement"],
            rightsElements[4],
        )

        file_id = self.source[0]
        embed_link = str(
            Part(
                index=1,
                url=f"https://babel.hathitrust.org/cgi/pt?id={file_id}",
                source=Source.HATHI.value,
                file_type="text/html",
                flags=str(FileFlags(embed=True)),
            )
        )

        self.record.has_part = [embed_link]

        # Check if digitization source column exists and is not Google before adding PDF link
        digitization_source = self.source[23].lower() if len(self.source) > 23 else ""
        if digitization_source != "google":
            file_link = str(
                Part(
                    index=1,
                    url=f"https://babel.hathitrust.org/cgi/imgsrv/download/pdf?id={file_id}",
                    source=Source.HATHI.value,
                    file_type="application/pdf",
                    flags=str(FileFlags(download=True)),
                )
            )

            self.record.has_part.append(file_link)

        # Parse spatial (pub place) codes
        self.record.spatial = self.record.spatial or ""
        self.record.spatial = self.constants["marc"]["countryCodes"].get(
            self.record.spatial.strip(), "xx"
        )
