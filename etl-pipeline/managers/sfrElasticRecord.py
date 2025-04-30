from elasticsearch.exceptions import ConnectionTimeout

from model import (
    ESWork,
    ESSubject,
    ESIdentifier,
    ESAgent,
    ESLanguage,
    ESRights,
    ESEdition,
    PerLanguageField,
)


class SFRElasticRecordManager:
    def __init__(self, dbWork):
        self.dbWork = dbWork
        self.work = None

    def getCreateWork(self):
        workData = {
            field: getattr(self.dbWork, field, None) for field in ESWork.getFields()
        }

        self.work = ESWork(**workData)

        self.enhanceWork()

    def saveWork(self, retries=0):
        try:
            self.work.save(pipeline="language_detector")
        except ConnectionTimeout as e:
            if retries >= 2:
                raise e

            self.saveWork(retries=retries + 1)

    def updateWork(self, data):
        for key, value in data.items():
            setattr(self.work, key, value)

    def enhanceWork(self):
        """Build an ElasticSearch object from the provided postgresql ORM
        object. This builds a single object from the related tables of the
        db object that can be indexed and searched in ElasticSearch.
        """
        self.work.date_created = self.dbWork.date_created
        self.work.date_modified = self.dbWork.date_modified

        self.work.title = PerLanguageField(default=self.work.title)

        self.setSortTitle()
        self.work.alt_titles = [
            PerLanguageField(default=a) for a in self.dbWork.alt_titles
        ]

        self.work.subjects = []
        for s in self.dbWork.subjects:
            subj = ESSubject(**s)
            subj.heading = PerLanguageField(default=subj.heading)
            self.work.subjects.append(subj)

        self.work.agents = [
            ESAgent(**SFRElasticRecordManager.addAgent(a, defaultRole="author"))
            for a in [*self.dbWork.authors, *self.dbWork.contributors]
        ]

        self.work.identifiers = [
            ESIdentifier(**dict(i)) for i in self.dbWork.identifiers
        ]

        self.work.languages = [
            ESLanguage(**l) for l in list(filter(None, self.dbWork.languages))
        ]

        for edition in self.work.editions:
            self.work.is_government_document = SFRElasticRecordManager.addGovDocStatus(
                edition.measurements
            )
            if self.work.is_government_document is True:
                break

        self.work.editions = [self.createEdition(e) for e in self.dbWork.editions]

    @staticmethod
    def addAgent(agent, defaultRole="author"):
        agent["sort_name"] = agent["name"].lower()
        agent["roles"] = agent.get("roles", [defaultRole])
        return agent

    @staticmethod
    def addGovDocStatus(measurements):
        """
        Iterates through each dictionary in the measurement arrays to return True if a gov doc type and value exists
        """

        if measurements != [] and measurements != [{}]:
            for measurement in measurements:
                if measurement["type"] == "government_document":
                    if measurement["value"] == "1":
                        return True
            return False

        else:
            return False

    def createEdition(self, edition):
        newEd = ESEdition(
            **{field: getattr(edition, field, None) for field in ESEdition.getFields()}
        )
        newEd.edition_id = newEd.id
        del newEd.id

        newEd.title = PerLanguageField(default=newEd.title)
        newEd.sub_title = PerLanguageField(default=newEd.sub_title)

        if edition.alt_titles:
            newEd.alt_titles = [a for a in edition.alt_titles]

        newEd.identifiers = [ESIdentifier(**dict(i)) for i in edition.identifiers]

        newEd.agents = [
            ESAgent(**SFRElasticRecordManager.addAgent(a, defaultRole="publisher"))
            for a in [*edition.publishers, *edition.contributors]
        ]

        newEd.rights = [ESRights(**r) for r in edition.rights]

        newEd.languages = [
            ESLanguage(**l) for l in list(filter(None, edition.languages))
        ]

        newEd.formats = [
            f for f in set(SFRElasticRecordManager.addAvailableFormats(edition.items))
        ]

        return newEd

    @staticmethod
    def addAvailableFormats(items):
        for item in items:
            for link in item.links:
                yield link.media_type

    def setSortTitle(self):
        if self.work.sort_title is None:
            self.work.sort_title = self.dbWork.title.lower()
