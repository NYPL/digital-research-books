from collections import defaultdict
import re
import string
import warnings

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction import DictVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.exceptions import ConvergenceWarning

from logger import create_log

logger = create_log(__name__)


class FeatureSelector(BaseEstimator, TransformerMixin):
    def __init__(self, key):
        self.key = key

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X[self.key]


class KMeansManager:
    def __init__(self, instances):
        self.instances = instances
        self.df = None
        self.clusters = defaultdict(list)

    def createPipeline(self, transformers):
        pipelineComponents = {
            "place": (
                "place",
                Pipeline(
                    [
                        ("selector", FeatureSelector(key="place")),
                        (
                            "tfidf",
                            TfidfVectorizer(
                                preprocessor=KMeansManager.pubProcessor,
                                strip_accents="unicode",
                                analyzer="char_wb",
                                ngram_range=(2, 4),
                            ),
                        ),
                    ]
                ),
            ),
            "publisher": (
                "publisher",
                Pipeline(
                    [
                        ("selector", FeatureSelector(key="publisher")),
                        (
                            "tfidf",
                            TfidfVectorizer(
                                preprocessor=KMeansManager.pubProcessor,
                                strip_accents="unicode",
                                analyzer="char_wb",
                                ngram_range=(2, 4),
                            ),
                        ),
                    ]
                ),
            ),
            "edition": (
                "edition",
                Pipeline(
                    [
                        ("selector", FeatureSelector(key="edition")),
                        (
                            "tfidf",
                            TfidfVectorizer(
                                preprocessor=KMeansManager.pubProcessor,
                                strip_accents="unicode",
                                analyzer="char_wb",
                                ngram_range=(1, 3),
                            ),
                        ),
                    ]
                ),
            ),
            "pubDate": (
                "pubDate",
                Pipeline(
                    [
                        ("selector", FeatureSelector(key="pubDate")),
                        ("vect", DictVectorizer()),
                    ]
                ),
            ),
        }

        pipelineWeights = {
            "place": 1.0,
            "publisher": 1.0,
            "edition": 1.0,
            "pubDate": 1.75,
        }

        return Pipeline(
            [
                (
                    "union",
                    FeatureUnion(
                        transformer_list=[pipelineComponents[t] for t in transformers],
                        transformer_weights={
                            t: pipelineWeights[t] for t in transformers
                        },
                    ),
                ),
                ("kmeans", KMeans(n_clusters=self.currentK, max_iter=100, n_init=3)),
            ]
        )

    @classmethod
    def pubProcessor(cls, raw):
        if isinstance(raw, list):
            raw = ", ".join(filter(None, raw))
        if raw is not None:
            raw = raw.replace("&", "and")
            cleanStr = raw.translate(str.maketrans("", "", string.punctuation)).lower()
            cleanStr = (
                cleanStr.replace("sn", "")
                .replace("place of publication not identified", "")
                .replace("publisher not identified", "")
            )
            cleanStr = re.sub(r"\s+", " ", cleanStr)
            return cleanStr
        logger.debug("Unable to clean NoneType, returning empty string")
        return ""

    def createDF(self):
        frameRows = []

        for i in self.instances:
            spatialData, dateData, publisherData = self.getInstanceData(i)

            if bool(spatialData) or dateData != {} or publisherData:
                frameRows.append(
                    {
                        "place": spatialData or "",
                        "publisher": publisherData,
                        "pubDate": dateData,
                        "edition": self.getEditionStatement(i.has_version),
                        "uuid": i.uuid,
                    }
                )

        self.df = pd.DataFrame(frameRows)

        self.maxK = len(self.df.index) if len(self.df.index) > 1 else 2

        if self.maxK > 5000:
            self.maxK = int(self.maxK * (1 / 9))
        elif self.maxK > 1000:
            self.maxK = int(self.maxK * (2 / 9))
        elif self.maxK > 500:
            self.maxK = int(self.maxK * (3 / 9))
        elif self.maxK > 250:
            self.maxK = int(self.maxK * (4 / 9))

        if self.maxK > 1000:
            self.maxK = 1000

    @classmethod
    def getInstanceData(cls, instance):
        spatial = instance.spatial
        date = cls.getPubDateObject(instance.dates)
        publisher = cls.getPublishers(instance.publisher)

        return (spatial, date, publisher)

    @classmethod
    def getPubDateObject(cls, dates):
        if dates is None or len(dates) < 1:
            return {}

        pubYears = {}

        for d in dates:
            try:
                date, dateType = tuple(d.split("|"))

                dateGroups = re.search(r"([\d\-\?]+)", date)

                dateStr = dateGroups.group(1)

                startYear, endYear = ("", "")

                if re.match(r"[0-9]{4}-[0-9]{4}", dateStr):
                    rangeMatches = re.match(r"([0-9]{4})-([0-9]{4})", dateStr)
                    startYear = rangeMatches.group(1)
                    endYear = rangeMatches.group(2)
                elif re.match(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", dateStr):
                    year, _, _ = tuple(dateStr.split("-"))
                    startYear, endYear = (year, year)
                elif re.match(r"[0-9]{4}-[0-9]{2}", dateStr):
                    year, _ = tuple(dateStr.split("-"))
                    startYear, endYear = (year, year)
                else:
                    startYear, endYear = (dateStr, dateStr)

                yearParser = YearObject(startYear, endYear)
                yearParser.setYearComponents()
                pubYears[dateType] = dict(yearParser)
            except (ValueError, AttributeError, IndexError) as e:
                logger.warning("Unable to parse date {}".format(d))

        for datePref in ["copyright_date", "publication_date", "issued"]:
            if datePref in pubYears.keys():
                return pubYears[datePref]

        logger.debug("Unable to locate publication date")
        return {}

    @classmethod
    def getPublishers(cls, publishers):
        if not publishers or len(publishers) < 1:
            return ""

        pubs = []
        for pub in publishers:
            if not pub:
                continue

            publisher, *_ = tuple(pub.split("|"))
            pubs.append(publisher.strip(",. []").lower())

        return ", ".join(pubs)

    @classmethod
    def getEditionStatement(cls, hasVersion):
        if not hasVersion:
            return ""
        for version in hasVersion:
            try:
                statement, _ = tuple(version.split("|"))
                return statement
            except ValueError:
                pass

        return ""

    def generateClusters(self):
        try:
            self.getK(2, self.maxK)
        except ZeroDivisionError:
            logger.warning("Single instance found - setting K to 1")
            self.k = 1

        try:
            labels = self.cluster(self.k)
        except ValueError as err:
            labels = [0] * len(self.instances)

        for n, item in enumerate(labels):
            try:
                self.clusters[item].append(self.df.loc[[n]])
            except KeyError:
                continue

    def getK(self, start, stop):
        warnings.filterwarnings("error", category=ConvergenceWarning)

        startScore = 0
        stopScore = 0

        prevStart = 0
        prevStop = 0

        while True:
            middle = int((stop + start) / 2)

            try:
                if start != prevStart:
                    startScore = self.cluster(start, score=True)
            except (ValueError, ConvergenceWarning):
                logger.debug("Exceeded number of distinct clusters, break")
                start = 1
                startScore = 1
                break

            try:
                if stop != prevStop:
                    stopScore = self.cluster(stop, score=True)
            except (ValueError, ConvergenceWarning):
                logger.debug("Exceeded number of distinct clusters, break")
                stop = middle
                continue

            if stop - start <= 1:
                break

            prevStart = start
            prevStop = stop

            if startScore > stopScore:
                stop = middle
            else:
                start = middle

        self.k = start if startScore > stopScore else stop

    def cluster(self, k, score=False):
        self.currentK = k
        logger.debug("Generating cluster for k={}".format(k))
        columnsWithData = self.getDataColumns()
        pipeline = self.createPipeline(columnsWithData)

        labels = pipeline.fit_predict(self.df)

        if score is True:
            pipeline.set_params(kmeans=None)
            X = pipeline.fit_transform(self.df)
            return silhouette_score(X, labels)
        else:
            return labels

    def getDataColumns(self):
        dataColumns = []
        for colName in self.df.columns:
            if colName == "uuid":
                continue

            hasValue = self.df[colName] != ""
            if len(list(filter(lambda x: x is True, list(hasValue.head())))) > 0:
                dataColumns.append(colName)

        return dataColumns

    def parseEditions(self):
        eds = []
        for clust in dict(self.clusters):
            yearEds = defaultdict(list)
            logger.debug("Parsing cluster {}".format(clust))
            for ed in self.clusters[clust]:
                editionYear = YearObject.convertYearDictToStr(ed.iloc[0]["pubDate"])
                logger.debug("Adding instance to {} edition".format(editionYear))
                yearEds[editionYear].append(ed.iloc[0]["uuid"])
            eds.extend([(year, data) for year, data in yearEds.items()])
            eds.sort(key=lambda x: x[0])

        return eds


class YearObject:
    def __init__(self, start, end):
        self.start = str(start)
        self.end = str(end)
        self.century = [None, None]
        self.decade = [None, None]
        self.year = [None, None]

    def setYearComponents(self):
        self.setCentury()
        self.setDecade()
        self.setYear()

    def setCentury(self):
        self.century[0] = int(self.start[:2])

        if len(self.end) > 2:
            self.century[1] = int(self.end[:2])

    def setDecade(self):
        if self.start[2] not in ["-", "?"]:
            self.decade[0] = int(self.start[2])

        if len(self.end) > 2 and self.end[2] not in ["-", "?"]:
            self.decade[1] = int(self.end[2])

    def setYear(self):
        if self.start[3] not in ["-", "?"]:
            self.year[0] = int(self.start[3])

        if len(self.end) > 2 and self.end[3] not in ["-", "?"]:
            self.year[1] = int(self.end[3])

    def __iter__(self):
        for key in ["century", "decade", "year"]:
            yearComp = getattr(self, key)

            if yearComp[0] is not None:
                yield "{}Start".format(key), yearComp[0]

            if yearComp[1] is not None:
                yield "{}End".format(key), yearComp[1]

    @staticmethod
    def convertYearDictToStr(yearDict):
        startYear = YearObject.getYearStr(yearDict, "Start")
        endYear = YearObject.getYearStr(yearDict, "End")

        return (
            "{}-{}".format(startYear, endYear)
            if endYear and endYear != startYear
            else str(startYear)
        )

    @staticmethod
    def getYearStr(yearDict, yearType):
        century = yearDict.get("century{}".format(yearType), None)
        decade = yearDict.get("decade{}".format(yearType), None)
        year = yearDict.get("year{}".format(yearType), None)

        return "".join(
            map(lambda x: str(x) if x is not None else "x", [century, decade, year])
        )
