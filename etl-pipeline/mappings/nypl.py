import requests

from .sql import SQLMapping


class NYPLMapping(SQLMapping):
    def __init__(self, source, bibItems, constants, locationCodes):
        super().__init__(source, constants)
        self.mapping = self.createMapping()
        self.locationCodes = locationCodes
        self.bibItems = bibItems

    def createMapping(self):
        return {
            "title": ("title", "{0}"),
            "alternative": [("246", "{a} {b} {p}"), ("247", "{a} {f}")],
            "authors": [("author", "{0}|||true")],
            "languages": [("lang", "{name}||{code}")],
            "dates": [
                ("publish_year", "{0}|publication_date"),
                ("catalog_date", "{0}|catalog_date"),
            ],
            "spatial": ("country", "{name}"),
            "identifiers": [
                ("id", "{0}|nypl"),
                ("issn", "{0}|issn"),
                ("lccn", "{0}|lccn"),
                ("oclc", "{0}|oclc"),
                ("isbn", "{0}|isbn"),
                ("010", "{a}|lccn"),
                ("020", "{a}|isbn"),
                ("022", "{a}|issn"),
                ("028", "{a}|{b}"),
                ("035", "{a}|scn"),
                ("050", "{a} {b}|lcc"),
                ("060", "{a}|nlmcn"),
                ("074", "{a}|gpoin"),
                ("086", "{a}|gdcn"),
            ],
            "publisher": [("260", "{b}||")],
            "contributors": [
                ("260", "{f}|||manufacturer"),
                ("700", "{a}|||{e}"),
                ("710", "{a} {b}|||{e}"),
                ("711", "{a} {b}|||{e}"),
            ],
            "has_version": ("250", "{a}|"),
            "extent": ("300", "{a}{b}{c}"),
            "is_part_of": [("440", "{a}|{v}|volume"), ("490", "{a}|{v}|volume")],
            "abstract": [("500", "{a}"), ("520", "{a} {b}")],
            "table_of_contents": ("505", "{a}"),
            "subjects": [
                ("600", "{a} {d} -- {t} -- {v}|lcsh|{0}"),
                ("610", "{a} -- {b} -- {v} -- {x} -- {z}|lcsh|{0}"),
                ("611", "{a} -- {v}|lcsh|{0}"),
                ("630", "{a} -- {p} -- {v}|lcsh|{0}"),
                ("650", "{a} -- {b} -- {v} -- {x} -- {z}|lcsh|{0}"),
                ("651", "{a} -- {b} -- {v} -- {x} -- {z}|lcsh|{0}"),
                ("655", "{a}|{2}|"),
                ("656", "{a}|{2}|"),
                ("690", "{a} -- {b} -- {v} -- {x} -- {z}|lcsh|{0}"),
            ],
            "has_part": [
                (
                    "856",
                    '1|{u}|nypl|text/html|{{"catalog": false, "download": false, "reader": false, "embed": true}}',
                )
            ],
        }

    def applyMapping(self):
        self.parseVarFields()
        super().applyMapping()

    def parseVarFields(self):
        self.source = dict(self.source)
        for value in self.source["var_fields"]:
            marcField = {
                "ind1": value.get("ind1", None),
                "ind2": value.get("ind2", None),
                "content": value.get("content", None),
            }
            try:
                for sub in value["subfields"]:
                    marcField[sub["tag"]] = sub.get("content", None)
            except (KeyError, TypeError):
                continue
            self.source[value.get("marcTag", "XXX")] = marcField

    def applyFormatting(self):
        self.record.source = "nypl"
        self.record.source_id = self.record.identifiers[0]
        self.record.coverage = []

        # Parse identifiers and de-duplicate
        cleanIdentifiers = set()
        for iden in self.record.identifiers:
            if isinstance(iden, list):
                cleanIdentifiers.update(iden)
            elif "(OCoLC)" in iden:
                oclcIden = iden.replace("(OCoLC)", "").replace("scn", "oclc")
                cleanIdentifiers.add(oclcIden)
            else:
                cleanIdentifiers.add(iden)
        self.record.identifiers = list(cleanIdentifiers)

        # Clean up subjects
        for i, subject in enumerate(self.record.subjects):
            subjComponents = subject.split("|")
            subjParts = list(
                filter(lambda x: x.strip() != "", subjComponents[0].split("--"))
            )
            self.record.subjects[i] = "{}|{}|{}".format(
                "--".join(subjParts), *subjComponents[1:]
            )

        # Parse contributors to set proper roles
        for i, contributor in enumerate(self.record.contributors):
            contribComponents = contributor.split("|")
            lcRelation = self.constants["lc"]["relators"].get(
                contribComponents[-1], "Contributor"
            )
            self.record.contributors[i] = "{}|{}|{}|{}".format(
                *contribComponents[:-1], lcRelation
            )

        # Add catalog link derived from nypl identifier if 856 field is not present
        if len(self.record.has_part) < 1:
            self.record.has_part.append(
                '1|{}|nypl|application/html+catalog|{{"catalog": true, "download": false, "reader": false}}'.format(
                    "https://www.nypl.org/research/collections/shared-collection-catalog/bib/b{}".format(
                        self.source["id"]
                    )
                )
            )

        # Add requestability status to each associated coverage location
        for item in self.bibItems:
            try:
                locationMetadata = self.locationCodes[item["location"]["code"]]
            except KeyError:
                continue

            if locationMetadata["requestable"]:
                pos = len(self.record.has_part) + 1

                self.record.coverage.append(
                    "{}|{}|{}".format(
                        item["location"]["code"], item["location"]["name"], pos
                    )
                )

                self.record.has_part.append(
                    '{}|{}|nypl|application/x.html+edd|{{"edd": true, "catalog": false, "download": false, "reader": false, "nypl_login": true}}'.format(
                        pos,
                        "http://www.nypl.org/research/collections/shared-collection-catalog/hold/request/b{}-i{}".format(
                            self.source["id"], item["id"]
                        ),
                    )
                )
