from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import json
from Levenshtein import jaro_winkler
import pycountry
import re
from uuid import uuid4

from model import Work, Edition, Item, Identifier, Link, Record, Rights
from logger import create_log

logger = create_log(__name__)


class SFRRecordManager:
    TITLE_STOPWORDS = {
        "eng": ["a", "an", "the"],
        "fra": ["le", "la", "les", "l", "un", "une"],
        "esp": ["el", "la", "los", "las", "un", "una"],
    }

    def __init__(self, session, iso639):
        self.session = session
        self.iso639_2b = iso639["2b"]
        self.work = Work(uuid=uuid4(), editions=[])

    def mergeRecords(self):
        dcdwUUIDs = set()

        for edition in self.work.editions:
            dcdwUUIDs.update(edition.dcdw_uuids)

        matchedWorks = []
        for matchedWork in (
            self.session.query(Work)
            .join(Edition)
            .filter(Work.uuid != self.work.uuid)
            .filter(Edition.dcdw_uuids.overlap(list(dcdwUUIDs)))
            .all()
        ):
            matchedWorks.append(
                (matchedWork.id, matchedWork.uuid, matchedWork.date_created)
            )

        matchedWorks.sort(key=lambda x: x[2])

        allIdentifiers = self.work.identifiers.copy()

        for edition in self.work.editions:
            allIdentifiers.extend(edition.identifiers)

            for item in edition.items:
                allIdentifiers.extend(item.identifiers)
                item.links = self.dedupeLinks(item.links)

        cleanIdentifiers = self.dedupeIdentifiers(allIdentifiers)

        self.seenIdentifiers = {}

        self.assignIdentifierIDs(cleanIdentifiers, self.work.identifiers)

        for edition in self.work.editions:
            self.assignIdentifierIDs(cleanIdentifiers, edition.identifiers)

            for item in edition.items:
                self.assignIdentifierIDs(cleanIdentifiers, item.identifiers)

        if len(matchedWorks) > 0:
            _, work_uuid, work_date_created = matchedWorks[0]
            self.work.uuid = work_uuid
            self.work.date_created = work_date_created

        self.work = self.session.merge(self.work)

        return [w[0] for w in matchedWorks]

    def dedupeIdentifiers(self, identifiers):
        queryGroups = defaultdict(set)
        cleanIdentifiers = {}

        for iden in identifiers:
            queryGroups[iden.authority].add(iden)

        for authority, identifiers in queryGroups.items():
            idenNos = [i.identifier for i in identifiers]
            for matchedID in (
                self.session.query(Identifier)
                .filter(Identifier.identifier.in_(idenNos))
                .filter(Identifier.authority == authority)
                .all()
            ):
                cleanIdentifiers[(authority, matchedID.identifier)] = matchedID.id

        return cleanIdentifiers

    def dedupeLinks(self, links):
        cleanLinks = set()
        for link in links:
            matchedLink = self.session.query(Link).filter(Link.url == link.url).first()
            if matchedLink:
                link.id = matchedLink.id
                cleanLinks.add(link)
            else:
                cleanLinks.add(link)

        return list(cleanLinks)

    def assignIdentifierIDs(self, existingIDs, identifiers):
        for i in range(len(identifiers)):
            iden = identifiers[i]

            try:
                seenID = self.seenIdentifiers[(iden.authority, iden.identifier)]
                identifiers[i] = seenID
                continue
            except KeyError:
                pass

            try:
                existingID = existingIDs[(iden.authority, iden.identifier)]
                iden.id = existingID
            except KeyError:
                pass

            self.seenIdentifiers[(iden.authority, iden.identifier)] = iden

    def buildEditionStructure(self, records, editions):
        logger.debug("Building Edition Structure")
        recordDict = {r.uuid: r for r in records}

        editionRecs = []
        workRecs = set(records)

        for editionTuple in editions:
            edYear, recs = editionTuple
            edRecs = [recordDict[rec] for rec in recs]
            editionRecs.append((edYear, edRecs))

            workRecs = workRecs - set(edRecs)

        logger.debug("Edition Structure Complete")
        return editionRecs, workRecs

    def buildWork(self, records, editions):
        editionRecs, workRecs = self.buildEditionStructure(records, editions)

        workData = SFRRecordManager.createEmptyWorkRecord()

        for workInstance in workRecs:
            self.addWorkInstanceMetadata(workData, workInstance)

        for pubYear, instances in editionRecs:
            self.buildEdition(workData, pubYear, instances)

        return workData

    def addWorkInstanceMetadata(self, workData, rec):
        workData["title"][rec.title] += 1

        workData["identifiers"].update(rec.identifiers)

        if rec.authors:
            workData["authors"].update(rec.authors)

        if rec.subjects:
            workData["subjects"].update(rec.subjects)

    def buildEdition(self, workData, pubYear, instances):
        editionData = SFRRecordManager.createEmptyEditionRecord()

        # Publication Year
        editionData["publication_date"] = pubYear

        for rec in instances:
            self.parseInstance(workData, editionData, rec)

        workData["editions"].append(editionData)

    def parseInstance(self, workData, editionData, rec):
        # Title Fields
        workData["title"][rec.title] += 1
        editionData["title"][rec.title] += 1
        if rec.alternative:
            for alt in rec.alternative:
                workData["alt_titles"][alt] += 1
                editionData["alt_titles"][alt] += 1

        # Medium
        workData["medium"][rec.medium] += 1

        # is_part_of (Series and Volume)
        if rec.is_part_of:
            for partOf in rec.is_part_of:
                if partOf[-6:] == "series":
                    workData["series_data"][partOf] += 1
                elif partOf[-6:] == "volume":
                    editionData["volume_data"][partOf] += 1

        # Edition
        if rec.has_version:
            editionData["edition_data"][rec.has_version] += 1

        # Identifiers
        workData["identifiers"].update(rec.identifiers)
        editionData["identifiers"].update(rec.identifiers)

        # Authors
        if rec.authors:
            workData["authors"].update(rec.authors)

        # Publishers
        if rec.publisher:
            editionData["publishers"].update(rec.publisher)

        # Publication_place
        editionData["publication_place"][rec.spatial] += 1

        # Contributors
        itemContributors = set()
        if rec.contributors:
            for contrib in rec.contributors:
                role = tuple(contrib.split("|"))[-1]

                if role in ["publisher", "manufacturer", "distributor", "printer"]:
                    editionData["contributors"].add(contrib)
                elif role in ["provider", "repository", "digitizer", "responsible"]:
                    itemContributors.add(contrib)
                else:
                    workData["contributors"].add(contrib)

        # Subjects
        if rec.subjects:
            workData["subjects"].update(rec.subjects)

        # Dates
        editionData["dates"].update(self.normalizeDates(rec.dates))

        # Languages
        if rec.languages:
            workData["languages"].update(rec.languages)
            editionData["languages"].update(rec.languages)

        # Summary/TOC/Extent
        editionData["summary"][rec.abstract] += 1
        editionData["table_of_contents"][rec.table_of_contents] += 1
        editionData["extent"][rec.extent] += 1

        # Measurements
        if rec.requires:
            for measure in rec.requires:
                _, measurement = tuple(measure.split("|"))
                if measurement == "government_doc":
                    workData["measurements"].add(measure)
                else:
                    editionData["measurements"].add(measure)

        # Item Records
        if rec.has_part:
            self.buildItems(editionData, rec, itemContributors)

        editionData["items"] = list(filter(None, editionData["items"]))

        editionData["dcdw_uuids"].append(rec.uuid.hex)

    def buildItems(self, editionData, rec: Record, itemContributors):
        number_of_parts = max(
            max(
                (
                    int(part.split("|")[0])
                    for part in rec.has_part
                    if part.split("|")[0].isdigit()
                ),
                default=0,
            ),
            len(rec.has_part),
        )

        startPos = len(editionData["items"]) - 1
        editionData["items"].extend([None] * number_of_parts)

        for item in rec.parts:
            try:
                linkFlags = json.loads(item.flags)
                linkFlags = linkFlags if isinstance(linkFlags, dict) else {}
            except json.decoder.JSONDecodeError:
                linkFlags = {}

            if linkFlags.get("cover", False) is True or "covers" in item.url:
                editionData["links"].append(
                    "{}|{}|{}".format(item.url, item.file_type, item.flags)
                )
                continue

            itemPos = startPos + int(item.index)
            if editionData["items"][itemPos] is None:
                editionData["items"][itemPos] = {
                    "links": [],
                    "identifiers": set(),
                    "contributors": set(),
                }
            editionData["items"][itemPos] = {
                **editionData["items"][itemPos],
                **{
                    "record_id": rec.id,
                    "source": item.source,
                    "publisher_project_source": rec.publisher_project_source,
                    "content_type": "ebook",
                    "modified": rec.date_submitted,
                    "rights": rec.rights,
                },
            }

            editionData["items"][itemPos]["links"].append(
                "{}|{}|{}".format(item.url, item.file_type, item.flags)
            )

            editionData["items"][itemPos]["identifiers"].update(
                [
                    i
                    for i in list(
                        filter(
                            lambda x: re.search(
                                r"\|(?!isbn|issn|oclc|lccn|owi|ddc|lcc|nypl).*$", x
                            ),
                            rec.identifiers,
                        )
                    )
                ]
            )

            editionData["items"][itemPos]["contributors"].update(itemContributors)

            if rec.coverage and len(rec.coverage) > 0:
                for location in rec.coverage:
                    locationCode, locationName, itemNo = tuple(location.split("|"))
                    if itemNo == str(item.index):
                        editionData["items"][itemPos]["physical_location"] = {
                            "code": locationCode,
                            "name": locationName,
                        }
                        break

    def saveWork(self, workData):
        # Set Titles
        try:
            self.work.title = workData["title"].most_common(1)[0][0].strip(" .:/")
        except IndexError:
            self.work.title = ""

        if len(workData["sub_title"]):
            self.work.sub_title = workData["sub_title"].most_common(1)[0][0]
        self.work.alt_titles = [t[0] for t in workData["alt_titles"].most_common()]

        # Set Agents
        self.work.authors = self.agentParser(
            workData["authors"], ["name", "viaf", "lcnaf", "primary"]
        )
        self.work.contributors = self.agentParser(
            workData["contributors"], ["name", "viaf", "lcnaf", "role"]
        )

        # Set Identifiers
        self.work.identifiers = SFRRecordManager.setPipeDelimitedData(
            workData["identifiers"], ["identifier", "authority"], Identifier
        )

        # Set Subjects
        self.subjectParser(workData["subjects"])

        # Set Measurements
        self.work.measurements = SFRRecordManager.setPipeDelimitedData(
            workData["measurements"], ["value", "type"]
        )

        # Set Languages
        self.work.languages = SFRRecordManager.setPipeDelimitedData(
            workData["languages"],
            ["language", "iso_2", "iso_3"],
            dParser=self.getLanguage,
        )

        # Set Medium
        try:
            self.work.medium = workData["medium"].most_common(1)[0][0]
        except IndexError:
            pass

        # Set Series
        if workData["series_data"]:
            series, seriesPos = tuple(
                workData["series_data"].most_common(1)[0][0].split("|")
            )
            self.work.series = series
            self.work.series_position = seriesPos

        # Add Editions
        for ed in workData["editions"]:
            self.work.editions.append(self.saveEdition(ed))

        # Set Sort Title
        self.setSortTitle()

    def saveEdition(self, edition):
        newEd = Edition(items=[], links=[])

        # Set Titles
        try:
            newEd.title = edition["title"].most_common(1)[0][0].strip(" .:/")
        except AttributeError:
            logger.warning("Unable to read title for edition")

        if len(edition["sub_title"]):
            newEd.sub_title = edition["sub_title"].most_common(1)[0][0]
        newEd.alt_titles = [t[0] for t in edition["alt_titles"].most_common()]

        # Set Publication Date
        newEd.publication_date = SFRRecordManager.publicationDateCheck(edition)

        # Set Publication Place
        newEd.publication_place = edition["publication_place"].most_common(1)[0][0]

        # Set Abstract Data
        newEd.table_of_contents = edition["table_of_contents"].most_common(1)[0][0]
        newEd.extent = edition["extent"].most_common(1)[0][0]
        newEd.summary = edition["summary"].most_common(1)[0][0]

        # Set Edition Data
        if len(edition["edition_data"]):
            editionStmt, editionNo, *_ = tuple(
                edition["edition_data"].most_common(1)[0][0].split("|")
            )
            newEd.edition_statement = editionStmt
            try:
                newEd.edition = int(editionNo)
            except ValueError:
                pass

        # Set Volume Data
        if len(edition["volume_data"]):
            volume, volumeNo, _ = tuple(
                edition["volume_data"].most_common(1)[0][0].split("|")
            )
            newEd.volume = "{}, {}".format(volume, volumeNo)

        # Set Agents
        newEd.contributors = self.agentParser(
            edition["contributors"], ["name", "viaf", "lcnaf", "role"]
        )
        newEd.publishers = self.agentParser(
            edition["publishers"], ["name", "viaf", "lcnaf"]
        )

        # Set Identifiers
        newEd.identifiers = SFRRecordManager.setPipeDelimitedData(
            edition["identifiers"], ["identifier", "authority"], Identifier
        )

        # Set Languages
        newEd.languages = SFRRecordManager.setPipeDelimitedData(
            edition["languages"],
            ["language", "iso_2", "iso_3"],
            dParser=self.getLanguage,
        )

        # Set Measurements
        newEd.measurements = SFRRecordManager.setPipeDelimitedData(
            edition["measurements"], ["value", "type"]
        )

        # Set Dates
        newEd.dates = SFRRecordManager.setPipeDelimitedData(
            edition["dates"], ["date", "type"]
        )

        # Set Links
        newEd.links = SFRRecordManager.setPipeDelimitedData(
            edition["links"],
            ["url", "media_type", "flags"],
            Link,
            dParser=SFRRecordManager.parseLinkFlags,
        )

        # Add Items
        for item in edition["items"]:
            newEd.items.append(self.saveItem(item))

        # Set DCDW UUIDs
        newEd.dcdw_uuids = list(edition["dcdw_uuids"])

        return newEd

    def saveItem(self, item):
        links = item.pop("links", [])
        identifiers = item.pop("identifiers", [])
        rights = item.pop("rights", [])
        newItem = Item(**item)

        # Set Links
        newItem.links = SFRRecordManager.setPipeDelimitedData(
            links,
            ["url", "media_type", "flags"],
            Link,
            dParser=SFRRecordManager.parseLinkFlags,
        )

        # Set Identifiers
        newItem.identifiers = SFRRecordManager.setPipeDelimitedData(
            identifiers, ["identifier", "authority"], Identifier
        )

        # Set Rights
        newItem.rights = SFRRecordManager.setPipeDelimitedData(
            rights if isinstance(rights, list) else [rights],
            ["source", "license", "rights_reason", "rights_statement", "rights_date"],
            Rights,
            dParser=lambda x: dict(list(filter(lambda y: y[1] != "", x.items()))),
        )

        # Set Contributors
        newItem.contributors = self.agentParser(
            item["contributors"], ["name", "viaf", "lcnaf", "role"]
        )

        return newItem

    @staticmethod
    # Exclude dates in the future and dates before the oldest book publisher company was founded
    def publicationDateCheck(edition):
        publicationDate = None

        if isinstance(edition["publication_date"], datetime):
            publicationDate = edition["publication_date"]
        elif re.match(r"([0-9]{4})-([0-9]{2})-([0-9]{2})", edition["publication_date"]):
            publicationDate = datetime.strptime(edition["publication_date"], "%Y-%m-%d")
        else:
            pubYearGroup = re.search(r"([0-9]{4})", str(edition["publication_date"]))

            if pubYearGroup:
                publicationDate = datetime(
                    year=int(pubYearGroup.group(1)), month=1, day=1
                )

        if (
            publicationDate
            and publicationDate <= datetime.now(timezone.utc).replace(tzinfo=None)
            and publicationDate.year >= 1488
        ):
            return publicationDate

        return None

    @staticmethod
    def setPipeDelimitedData(data, fields, dType=None, dParser=None):
        return list(
            filter(
                None,
                [
                    SFRRecordManager.parseDelimitedEntry(d, fields, dType, dParser)
                    for d in list(filter(None, data))
                ],
            )
        )

    @staticmethod
    def parseDelimitedEntry(dInst, fields, dType, dParser):
        dataEntry = dict(zip(fields, dInst.split("|")))
        if dParser is not None:
            dataEntry = dParser(dataEntry)
        if dType is None:
            return dataEntry

        return dType(**dataEntry)

    def getLanguage(self, langData):
        values = list(filter(lambda x: x != "", [v for _, v in langData.items()]))
        for v in values:
            for attr in ["alpha_2", "alpha_3", "name"]:
                if attr == "alpha_3":
                    v = self.iso639_2b.get(v, v)

                pyLang = pycountry.languages.get(**{attr: v})

                if pyLang is not None:
                    return {
                        "language": pyLang.name,
                        "iso_2": getattr(pyLang, "alpha_2", None),
                        "iso_3": getattr(pyLang, "alpha_3", None),
                    }

    @staticmethod
    def parseLinkFlags(linkData):
        try:
            linkData["flags"] = json.loads(linkData["flags"])
        except json.decoder.JSONDecodeError:
            linkData["flags"] = {}

        return linkData

    def agentParser(self, agents, fields):
        outAgents = {}

        for agent in agents:
            if agent is None:
                continue

            agentParts = agent.split("|")

            while len(agentParts) < len(fields):
                agentParts.insert(1, "")

            rec = dict(zip(fields, agentParts))
            recKey = re.sub(r"[.,:\(\)\-0-9]+", "", rec["name"].lower())

            if rec["name"] == "" and rec["viaf"] == "" and rec["lcnaf"] == "":
                continue

            existingMatch = False
            for oaKey, oa in outAgents.items():
                for checkField in ["viaf", "lcnaf"]:
                    if (
                        rec[checkField]
                        and rec[checkField] != ""
                        and rec[checkField] == oa[checkField]
                    ):
                        existingMatch = True
                        SFRRecordManager.mergeAgents(oa, rec)
                        break

                if existingMatch is False:
                    if jaro_winkler(oaKey, recKey) > 0.74:
                        SFRRecordManager.mergeAgents(oa, rec)
                        existingMatch = True
                        break

            if existingMatch is False:
                if "role" in rec.keys():
                    rec["roles"] = list(set([rec["role"]]))
                    del rec["role"]
                outAgents[recKey] = rec

        return [a for _, a in outAgents.items()]

    def subjectParser(self, subjects):
        cleanSubjects = {}
        for subj in subjects:
            try:
                heading, auth, authNo = subj.split("|")
            except ValueError:
                authNoRev, authRev, *headRevArr = subj[::-1].split("|")
                authNo = authNoRev[::-1]
                auth = authRev[::-1]
                heading = ",".join(headRevArr)[::-1]

            if heading == "":
                continue

            cleanHeading = heading.strip(",.")
            headingKey = cleanHeading.lower()

            if headingKey in cleanSubjects.keys():
                if cleanSubjects[headingKey][1] == "" and auth != "":
                    cleanSubjects[headingKey][1] = auth
                    cleanSubjects[headingKey][2] = authNo
            else:
                cleanSubjects[headingKey] = [cleanHeading, auth, authNo]

        cleanSubjStrs = ["|".join(subj) for _, subj in cleanSubjects.items()]
        self.work.subjects = SFRRecordManager.setPipeDelimitedData(
            cleanSubjStrs, ["heading", "authority", "controlNo"]
        )

    def normalizeDates(self, dates):
        outDates = set()

        if dates is None:
            return list()

        for date in dates:
            try:
                dateValue, dateType = tuple(date.split("|"))

                cleanDate = re.search(r"\d+.*\d+", dateValue).group()

                outDates.add("{}|{}".format(cleanDate, dateType))
            except (AttributeError, ValueError):
                pass

        return list(outDates)

    @staticmethod
    def mergeAgents(existing, new):
        if "viaf" in new.keys() and new["viaf"] != "":
            existing["viaf"] = new["viaf"]

        if "lcnaf" in new.keys() and new["lcnaf"] != "":
            existing["lcnaf"] = new["lcnaf"]

        if "primary" in new.keys() and new["primary"] != "":
            existing["primary"] = new["primary"]

        if "role" in new.keys():
            roleSet = set(existing["roles"])
            roleSet.add(new["role"])
            existing["roles"] = list(roleSet)

    @staticmethod
    def createEmptyWorkRecord():
        return {
            "title": Counter(),
            "sub_title": Counter(),
            "alt_titles": Counter(),
            "medium": Counter(),
            "series_data": Counter(),
            "identifiers": set(),
            "authors": set(),
            "contributors": set(),
            "subjects": set(),
            "dates": set(),
            "languages": set(),
            "measurements": set(),
            "editions": [],
        }

    @staticmethod
    def createEmptyEditionRecord():
        return {
            "title": Counter(),
            "sub_title": Counter(),
            "alt_titles": Counter(),
            "edition_data": Counter(),
            "publication_place": Counter(),
            "publication_date": None,
            "volume_data": Counter(),
            "table_of_contents": Counter(),
            "extent": Counter(),
            "summary": Counter(),
            "identifiers": set(),
            "publishers": set(),
            "contributors": set(),
            "dates": set(),
            "measurements": set(),
            "languages": set(),
            "items": [],
            "links": [],
            "dcdw_uuids": [],
        }

    def setSortTitle(self):
        stops = [
            s
            for l in [l["iso_3"] if l else None for l in self.work.languages]
            for s in self.TITLE_STOPWORDS.get(l, [])
        ]

        tokens = re.split(r"\s+", self.work.title)

        self.work.sort_title = " ".join(
            tokens[1:] if tokens[0].lower() in stops else tokens
        ).lower()
