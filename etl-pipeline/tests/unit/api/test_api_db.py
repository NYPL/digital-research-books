from datetime import date
import pytest
from sqlalchemy.sql import text

from api.db import DBClient
from api.utils import APIUtils


class TestDBClient:
    @pytest.fixture
    def testInstance(self, mocker):
        class MockDBClient(DBClient):
            def __init__(self, engine):
                self.engine = mocker.MagicMock()
                self.session = mocker.MagicMock()

        return MockDBClient("testEngine")

    @pytest.fixture
    def testCountQuery(self):
        return text("""SELECT relname AS table, reltuples AS row_count
            FROM pg_class c JOIN pg_namespace n ON (n.oid = c.relnamespace)
            WHERE nspname NOT IN ('pg_catalog', 'information_schema')
            AND relkind = 'r'
            AND relname IN ('records', 'works', 'editions', 'items', 'links')
        """)

    def test_createSession(self, testInstance, mocker):
        mockCreator = mocker.MagicMock()
        mockMaker = mocker.patch("api.db.sessionmaker")
        mockMaker.return_value = mockCreator
        mockSession = mocker.MagicMock()
        mockCreator.return_value = mockSession

        testInstance.createSession()

        assert testInstance.session == mockSession
        mockMaker.assert_called_once_with(bind=testInstance.engine)
        mockCreator.assert_called_once()

    def test_closeSession(self, testInstance):
        testInstance.closeSession()

        testInstance.session.close.assert_called_once()

    def test_fetchSearchedWorks(self, testInstance, mocker):
        testInstance.session.query().join().options().filter().all.return_value = [
            "work1",
            "work3",
        ]

        mockFlatten = mocker.patch.object(APIUtils, "flatten")
        mockFlatten.return_value = [1, 2, 3]

        workResult = testInstance.fetchSearchedWorks(
            [("uuid1", "ed1"), ("uuid2", "ed2"), ("uuid3", "ed3")]
        )

        assert workResult == ["work1", "work3"]
        testInstance.session.query().join().options().filter().all.assert_called_once()

    def test_fetchSingleWork(self, testInstance, mocker):
        testInstance.session.query().options().filter().first.return_value = "testWork"

        workResult = testInstance.fetchSingleWork("uuid")

        assert workResult == "testWork"
        testInstance.session.query().options().filter().first.assert_called_once()

    def test_fetchSingleEdition(self, testInstance, mocker):
        testInstance.session.query().options().filter().first.return_value = (
            "testEdition"
        )

        editionResult = testInstance.fetchSingleEdition("editionID")

        assert editionResult == "testEdition"
        testInstance.session.query().options().filter().first.assert_called_once()

    def test_fetchSingleLink(self, testInstance, mocker):
        testInstance.session.query().filter().first.return_value = "testLink"

        editionResult = testInstance.fetchSingleLink("linkID")

        assert editionResult == "testLink"
        testInstance.session.query().filter().first.assert_called_once()

    def test_fetchRecordsByUUID(self, testInstance, mocker):
        testInstance.session.query().filter().all.return_value = "testRecords"

        editionResult = testInstance.fetchRecordsByUUID(["uuid1", "uuid2"])

        assert editionResult == "testRecords"
        testInstance.session.query().filter().all.assert_called_once()

    def test_fetchRowCounts(self, testInstance, testCountQuery, mocker):
        testInstance.session.execute.return_value = "testCounts"

        totalResult = testInstance.fetchRowCounts()

        assert totalResult == "testCounts"
        testInstance.session.execute.call_args[0][0].compare(testCountQuery)

    def test_fetchNewWorks(self, testInstance):
        testInstance.session.query().filter().count.return_value = 1
        testInstance.session.query().filter().offset().limit().all.return_value = (
            "testWorks"
        )

        testResult = testInstance.fetchNewWorks()

        assert testResult[0] == 1
        assert testResult[1] == "testWorks"

        testInstance.session.query().filter().count.assert_called_once()
        testInstance.session.query().filter().offset().limit().all.assert_called_once()

    def test_fetchSingleCollection(self, testInstance):
        testInstance.session.query().options().filter().one.return_value = (
            "testCollection"
        )

        assert testInstance.fetchSingleCollection("uuid") == "testCollection"

        testInstance.session.query().options().filter().one.assert_called_once()

    def test_createStaticCollection(self, testInstance, mocker):
        mockUUID = mocker.patch("api.db.uuid4")
        mockUUID.return_value = "testUUID"

        mockCollInstance = mocker.MagicMock()
        mockCollection = mocker.patch("api.db.Collection")
        mockCollection.return_value = mockCollInstance

        mockWorkEditions = [
            mocker.MagicMock(id=1, publication_date=date(2000, 1, 1), items=["item1"]),
            mocker.MagicMock(id=2, publication_date=None, items=["item2"]),
            mocker.MagicMock(id=3, publication_date=date(1900, 1, 1), items=[]),
        ]
        mockWork = mocker.MagicMock(editions=mockWorkEditions)
        testInstance.session.query().join().filter().all.return_value = [mockWork]

        mockEditions = [mocker.MagicMock(id=4), mocker.MagicMock(id=5)]
        testInstance.session.query().filter().all.return_value = mockEditions

        testNewCollection = testInstance.createStaticCollection(
            "Test Coll",
            "Test Creator",
            "Test Description",
            "testOwner",
            workUUIDs=["testUUID"],
            editionIDs=["ed1", "ed2"],
        )

        assert len(testNewCollection.editions) == 3
        assert testNewCollection.editions[0].id == 1
        assert testNewCollection.editions[1].id == 4

        mockCollection.assert_called_once_with(
            uuid="testUUID",
            title="Test Coll",
            creator="Test Creator",
            description="Test Description",
            owner="testOwner",
            type="static",
        )

        testInstance.session.query().join().filter().all.assert_called_once()
        testInstance.session.query().filter().all.assert_called_once()
        testInstance.session.add.assert_called_once_with(mockCollInstance)

    def test_deleteCollection(self, testInstance):
        testInstance.session.query().filter().delete.return_value = "testDelete"

        assert testInstance.deleteCollection("uuid") == "testDelete"

        testInstance.session.query().filter().delete.assert_called_once()

    def test_fetchUser(self, testInstance):
        testInstance.session.query().filter().one_or_none.return_value = "testUser"

        assert testInstance.fetchUser("user") == "testUser"

        testInstance.session.query().filter().one_or_none.assert_called_once()
