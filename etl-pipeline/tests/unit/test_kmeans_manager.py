import pandas as pd
import pytest

from managers.kMeans import KMeansManager, YearObject


class TestKMeansModel(object):
    @pytest.fixture
    def testModel(self, testInstances, testClusters):
        testModel = KMeansManager([])
        testModel.instances = testInstances
        testModel.clusters = testClusters
        testModel.currentK = 1
        return testModel

    @pytest.fixture
    def testInstances(self, mocker):
        return [
            TestKMeansModel.createInstance(
                mocker,
                spatial="test",
                dates=["date1", "date2"],
                publisher=["agent1", "agent2"],
                edition=["edition1"],
                uuid=1,
            ),
            TestKMeansModel.createInstance(
                mocker,
                spatial=False,
                dates=None,
                publisher=None,
                edition=["edition1"],
                uuid=2,
            ),
            TestKMeansModel.createInstance(
                mocker,
                spatial="test",
                dates=["date1"],
                publisher=["agent2", "agent3"],
                edition=["edition1"],
                uuid=3,
            ),
        ]

    @pytest.fixture
    def testClusters(self):
        return {
            0: [
                pd.DataFrame(
                    {
                        "pubDate": 1900,
                        "publisher": "test",
                        "place": "testtown",
                        "uuid": 1,
                        "edition": "",
                        "volume": "",
                        "extent": "",
                        "table_of_contents": "",
                        "summary": "",
                    },
                    index=[0],
                ),
                pd.DataFrame(
                    {
                        "pubDate": 1900,
                        "publisher": "test",
                        "place": "testtown",
                        "uuid": 2,
                        "edition": "",
                        "volume": "",
                        "extent": "",
                        "table_of_contents": "",
                        "summary": "",
                    },
                    index=[0],
                ),
            ],
            1: [
                pd.DataFrame(
                    {
                        "pubDate": 2000,
                        "publisher": "test",
                        "place": "testtown",
                        "uuid": 3,
                        "edition": "",
                        "volume": "",
                        "extent": "",
                        "table_of_contents": "",
                        "summary": "",
                    },
                    index=[0],
                ),
                pd.DataFrame(
                    {
                        "pubDate": 1950,
                        "publisher": "test",
                        "place": "testtown",
                        "uuid": 4,
                        "edition": "",
                        "volume": "",
                        "extent": "",
                        "table_of_contents": "",
                        "summary": "",
                    },
                    index=[0],
                ),
            ],
        }

    @pytest.fixture
    def TestYear(self):
        class TestYear:
            def __init__(self, century, decade, year):
                self.century = century
                self.decade = decade
                self.year = year

            def setYearComponents(self):
                pass

            def __iter__(self):
                for el in ["century", "decade", "year"]:
                    yield el, getattr(self, el)

        return TestYear

    @staticmethod
    def createInstance(mocker, **kwargs):
        mockInst = mocker.MagicMock()
        for key, value in kwargs.items():
            setattr(mockInst, key, value)
        return mockInst

    def test_kModel_init(self, testModel):
        assert len(testModel.instances) == 3
        assert testModel.df == None
        assert isinstance(testModel.clusters, dict)

    def test_createPipeline(self, testModel, mocker):
        mockPipeline = mocker.patch("managers.kMeans.Pipeline")
        mockPipeline.side_effect = [
            "placePipe",
            "pubPipe",
            "edPipe",
            "datePipe",
            "mainPipe",
        ]
        mockFeatureUnion = mocker.patch("managers.kMeans.FeatureUnion")
        mockFeatureUnion.return_value = "testUnion"
        mockKMeans = mocker.patch("managers.kMeans.KMeans")
        mockKMeans.return_value = "testKMeans"

        testPipeline = testModel.createPipeline(["place", "publisher"])

        assert testPipeline == "mainPipe"
        mockFeatureUnion.assert_called_once_with(
            transformer_list=[("place", "placePipe"), ("publisher", "pubPipe")],
            transformer_weights={"place": 1.0, "publisher": 1.0},
        )
        mockKMeans.assert_called_once_with(n_clusters=1, max_iter=100, n_init=3)
        assert mockPipeline.call_args == mocker.call(
            [("union", "testUnion"), ("kmeans", "testKMeans")]
        )

    def test_pubProcessor_str(self):
        cleanStr = KMeansManager.pubProcessor("Testing & Testing,")
        assert cleanStr == "testing and testing"

        cleanStr2 = KMeansManager.pubProcessor("[publisher not identified]")
        assert cleanStr2 == ""

    def test_pubProcessor_list(self):
        cleanStr = KMeansManager.pubProcessor(["hello", "sn", "goodbye"])
        assert cleanStr == "hello goodbye"

    def test_pubProcessor_none(self):
        cleanStr = KMeansManager.pubProcessor(None)
        assert cleanStr == ""

    def test_createDF(self, mocker, testModel):
        mockGetData = mocker.patch.object(KMeansManager, "getInstanceData")
        mockGetData.side_effect = [
            (None, {"century": 20, "decade": 0, "year": 5}, "pub2"),
            (None, {}, None),
            ("place2", {"century": 19, "decade": 5, "year": 1}, None),
        ]
        mockGetEd = mocker.patch.object(KMeansManager, "getEditionStatement")
        mockGetEd.side_effect = ["ed1", "ed2", False]

        testModel.createDF()
        assert isinstance(testModel.df, pd.DataFrame)
        assert testModel.df.iloc[0]["uuid"] == 1
        assert testModel.df.iloc[1]["uuid"] == 3
        assert testModel.maxK == 2

    def test_createDF_maxK_1000_plus(self, mocker, testModel):
        testModel.instances = [
            mocker.MagicMock(uuid=i, has_version=i) for i in range(1200)
        ]
        mockGetData = mocker.patch.object(KMeansManager, "getInstanceData")
        mockGetData.side_effect = [(i, i, i) for i in range(1200)]
        mocker.patch.object(KMeansManager, "getEditionStatement")

        testModel.createDF()

        assert testModel.maxK == int(1200 * (2 / 9))

    def test_createDF_maxK_500_1000(self, mocker, testModel):
        testModel.instances = [
            mocker.MagicMock(uuid=i, has_version=i) for i in range(750)
        ]
        mockGetData = mocker.patch.object(KMeansManager, "getInstanceData")
        mockGetData.side_effect = [(i, i, i) for i in range(750)]
        mocker.patch.object(KMeansManager, "getEditionStatement")

        testModel.createDF()

        assert testModel.maxK == int(750 * (3 / 9))

    def test_createDF_maxK_250_500(self, mocker, testModel):
        testModel.instances = [
            mocker.MagicMock(uuid=i, has_version=i) for i in range(350)
        ]
        mockGetData = mocker.patch.object(KMeansManager, "getInstanceData")
        mockGetData.side_effect = [(i, i, i) for i in range(350)]
        mocker.patch.object(KMeansManager, "getEditionStatement")

        testModel.createDF()

        assert testModel.maxK == int(350 * (4 / 9))

    def test_getPubDateObject_range(self, mocker, TestYear):
        mockParser = mocker.patch("managers.kMeans.YearObject")
        mockParser.side_effect = [TestYear(20, 0, 0), TestYear(19, 0, 0)]

        mockDates = ["2000|other_date", "1900-1901|publication_date"]
        outYear = KMeansManager.getPubDateObject(mockDates)

        assert outYear == {"century": 19, "decade": 0, "year": 0}

    def test_getPubDateObject_year_only(self, mocker, TestYear):
        mockParser = mocker.patch("managers.kMeans.YearObject")
        mockParser.side_effect = [TestYear(20, 0, 0), TestYear(19, 0, 0)]

        mockDates = ["2000|other_date", "1900|publication_date"]
        outYear = KMeansManager.getPubDateObject(mockDates)

        assert outYear == {"century": 19, "decade": 0, "year": 0}

    def test_getPubDateObject_full_date(self, mocker, TestYear):
        mockParser = mocker.patch("managers.kMeans.YearObject")
        mockParser.side_effect = [TestYear(20, 0, 0), TestYear(19, 0, 0)]

        mockDates = ["2000-12-01|other_date", "1900-01-01|publication_date"]
        outYear = KMeansManager.getPubDateObject(mockDates)

        assert outYear == {"century": 19, "decade": 0, "year": 0}

    def test_getPubDateFloat_neither(self, mocker):
        mocker.patch("managers.kMeans.YearObject")
        mockDates = ["2000|other_date", "1900-1901|pub_date"]
        outYear = KMeansManager.getPubDateObject(mockDates)
        assert outYear == {}

    def test_getPubDateFloat_no_dates(self, mocker):
        outYear = KMeansManager.getPubDateObject([])
        assert outYear == {}

    def test_getPubDateFloat_bad_value(self, mocker):
        outYear = KMeansManager.getPubDateObject(["2000"])
        assert outYear == {}

    def test_getPubDateFloat_bad_string_value(self, mocker):
        outYear = KMeansManager.getPubDateObject(["test|date"])
        assert outYear == {}

    def test_getPublishers_clean(self, mocker):
        outPublisher = KMeansManager.getPublishers(["Test|||", "Other|||"])

        assert outPublisher == "test, other"

    def test_getPublishers_stripped(self, mocker):
        outPublisher = KMeansManager.getPublishers(["Test Second [].||viaf|"])

        assert outPublisher == "test second"

    def test_getPublishers_none(self, mocker):
        outPublisher = KMeansManager.getPublishers(None)

        assert outPublisher == ""

    def test_getEditionStatement_value_present(self, mocker):
        edStmt = KMeansManager.getEditionStatement(["edition|1"])

        assert edStmt == "edition"

    def test_getEditionStatement_no_value(self, mocker):
        edStmt = KMeansManager.getEditionStatement(None)

        assert edStmt == ""

    def test_getEditionStatement_empty_array(self, mocker):
        edStmt = KMeansManager.getEditionStatement([])

        assert edStmt == ""

    def test_generateClusters_multiple(self, mocker, testModel):
        mockGetK = mocker.patch.object(KMeansManager, "getK")
        testModel.k = 2
        testModel.maxK = 3

        mockCluster = mocker.patch.object(KMeansManager, "cluster")
        mockCluster.return_value = [0, 1, 0]

        testModel.df = pd.DataFrame(["row1", "row2", "row3"])

        testModel.generateClusters()
        assert testModel.clusters[0][0].iloc[0][0] == 1900
        assert testModel.clusters[0][1].iloc[0][0] == 1900
        assert testModel.clusters[1][0].iloc[0][0] == 2000

    def test_generateClusters_single(self, mocker, testModel):
        mockGetK = mocker.patch.object(KMeansManager, "getK")
        mockGetK.side_effect = ZeroDivisionError

        mockCluster = mocker.patch.object(KMeansManager, "cluster")
        mockCluster.side_effect = ValueError
        testModel.instances = ["row1"]
        testModel.df = pd.DataFrame(["row1"])
        testModel.maxK = 3

        testModel.generateClusters()
        assert testModel.clusters[0][0].iloc[0][0] == 1900

    def test_cluster_score(self, mocker, testModel):
        mockPipeline = mocker.MagicMock()
        mockPipeline.fit_transform.return_value = "testLabels"

        mockCreate = mocker.patch.object(KMeansManager, "createPipeline")
        mockCreate.return_value = mockPipeline
        mockGetColumns = mocker.patch.object(KMeansManager, "getDataColumns")
        mockSilhouette = mocker.patch("managers.kMeans.silhouette_score")
        mockSilhouette.return_value = 1

        out = testModel.cluster(1, score=True)
        assert out == 1
        mockGetColumns.assert_called_once()
        mockPipeline.set_params.assert_called_once_with(kmeans=None)
        mockPipeline.fit_transform.assert_called_once()
        mockSilhouette.assert_called_once()

    def test_cluster_predict(self, mocker, testModel):
        mockPipeline = mocker.MagicMock()
        mockCreate = mocker.patch.object(KMeansManager, "createPipeline")
        mockGetColumns = mocker.patch.object(KMeansManager, "getDataColumns")
        mockCreate.return_value = mockPipeline

        testModel.cluster(1)
        mockCreate.assert_called_once()
        mockGetColumns.assert_called_once()
        mockPipeline.fit_predict.assert_called_once()

    def test_parseEditions(self, testModel, mocker):
        mockConvert = mocker.patch.object(YearObject, "convertYearDictToStr")
        mockConvert.side_effect = [1900, 1900, 2000, 1950]
        outEditions = testModel.parseEditions()
        assert len(outEditions) == 3
        assert outEditions[1][0] == 1950
