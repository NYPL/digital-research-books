import pytest
from datetime import datetime, timedelta, timezone
from sqlalchemy import column
from unittest.mock import ANY

from model import Edition, Link
from processes import CoverProcess
from processes.utils import ProcessParams


class TestCoverProcess:
    @pytest.fixture
    def testProcess(self, mocker):
        class TestCoverProcess(CoverProcess):
            def __init__(self, *args):
                self.params = ProcessParams()
                self.db_manager = mocker.MagicMock()
                self.fileBucket = "test_aws_bucket"
                self.batchSize = 3
                self.runTime = datetime.now()
                self.redis_manager = mocker.MagicMock()
                self.s3_manager = mocker.MagicMock(client=mocker.MagicMock())
                self.editions_to_update = set()

        return TestCoverProcess()

    def test_runProcess(self, testProcess, mocker):
        processMocks = mocker.patch.multiple(
            CoverProcess,
            generate_query=mocker.DEFAULT,
            get_edition_covers=mocker.DEFAULT,
        )
        processMocks["generate_query"].return_value = "testQuery"

        testProcess.runProcess()

        processMocks["generate_query"].assert_called_once()
        processMocks["get_edition_covers"].assert_called_once_with("testQuery")

    def test_generate_query_complete(self, testProcess: CoverProcess, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.filter.return_value = "testQuery"

        mockSubQuery = mocker.MagicMock()
        mockSubQuery.join().distinct().filter.return_value = ["sub"]

        testProcess.db_manager.session = mocker.MagicMock()
        testProcess.db_manager.session.query.side_effect = [mockQuery, mockSubQuery]

        testProcess.params.process_type = "complete"
        testQuery = testProcess.generate_query()

        assert testQuery == "testQuery"
        assert mockQuery.filter.call_args[0][0].compare(~Edition.id.in_(["sub"]))
        assert testProcess.db_manager.session.query.call_count == 2
        mockSubQuery.join().distinct().filter.call_args[0][0].compare(
            Link.flags["cover"] == "true"
        )

    def test_generate_query_custom_date(self, testProcess: CoverProcess, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.filter.return_value = "testQuery"

        mockSubQuery = mocker.MagicMock()
        mockSubQuery.join().distinct().filter.return_value = ["sub"]

        testProcess.db_manager.session = mocker.MagicMock()
        testProcess.db_manager.session.query.side_effect = [mockQuery, mockSubQuery]

        testProcess.params.process_type = "custom"
        testProcess.params.ingest_period = "2020-01-01"
        testQuery = testProcess.generate_query()

        assert testQuery == "testQuery"
        assert mockQuery.filter.call_args[0][0].compare(~Edition.id.in_(["sub"]))
        testDate = datetime.strptime("2020-01-01", "%Y-%m-%d")
        assert mockQuery.filter.call_args[0][1].compare(
            Edition.date_modified >= testDate
        )

    def test_generate_query_daily(self, testProcess: CoverProcess, mocker):
        mockQuery = mocker.MagicMock()
        mockQuery.filter.return_value = "testQuery"

        mockSubQuery = mocker.MagicMock()
        mockSubQuery.join().distinct().filter.return_value = ["sub"]

        testProcess.db_manager.session.query.side_effect = [mockQuery, mockSubQuery]

        testProcess.params.process_type = "daily"
        testProcess.params.ingest_period = None
        testQuery = testProcess.generate_query()

        assert testQuery == "testQuery"
        assert mockQuery.filter.call_args[0][0].compare(~Edition.id.in_(["sub"]))
        assert mockQuery.filter.call_args[0][1] is not None

    def test_get_edition_covers(self, testProcess: CoverProcess, mocker):
        processMocks = mocker.patch.multiple(
            CoverProcess,
            search_for_cover=mocker.DEFAULT,
            store_cover=mocker.DEFAULT,
        )
        processMocks["search_for_cover"].side_effect = ["manager1", None, "manager2"]
        testProcess.db_manager.windowed_query.return_value = ["ed1", "ed2", "ed3"]

        testProcess.run_start_time = datetime.now(timezone.utc).replace(tzinfo=None)

        testProcess.get_edition_covers("mockQuery")

        processMocks["search_for_cover"].assert_has_calls(
            [mocker.call("ed1"), mocker.call("ed2"), mocker.call("ed3")]
        )
        processMocks["store_cover"].assert_has_calls(
            [mocker.call("manager1", "ed1"), mocker.call("manager2", "ed3")]
        )

    def test_get_edition_covers_timeout(self, testProcess: CoverProcess, mocker):
        processMocks = mocker.patch.multiple(
            CoverProcess,
            search_for_cover=mocker.DEFAULT,
            store_cover=mocker.DEFAULT,
        )
        processMocks["search_for_cover"].side_effect = ["manager1", None, "manager2"]
        testProcess.db_manager.windowed_query.return_value = ["ed1", "ed2", "ed3"]

        testProcess.run_start_time = datetime.now() - timedelta(13)

        testProcess.get_edition_covers("mockQuery")

        processMocks["search_for_cover"].assert_called_once_with("ed1")
        processMocks["store_cover"].assert_called_once_with("manager1", "ed1")

    def test_search_for_cover_success(self, testProcess: CoverProcess, mocker):
        mockIdentifierGet = mocker.patch.object(CoverProcess, "get_edition_identifiers")
        mockIdentifierGet.return_value = [(1, "test")]

        mockManager = mocker.MagicMock()
        mockManager.fetch_cover.return_value = True

        mockGenerator = mocker.patch("processes.file.covers.CoverManager")
        mockGenerator.return_value = mockManager

        testManager = testProcess.search_for_cover("testEdition")

        assert testManager == mockManager
        mockIdentifierGet.assert_called_once_with("testEdition")
        mockGenerator.assert_called_once_with(
            [(1, "test")], testProcess.db_manager.session
        )
        mockManager.fetch_cover.assert_called_once()
        mockManager.fetch_cover_file.assert_called_once()
        mockManager.resize_cover_file.assert_called_once()

    def test_search_for_cover_missing(self, testProcess: CoverProcess, mocker):
        mockIdentifierGet = mocker.patch.object(CoverProcess, "get_edition_identifiers")
        mockIdentifierGet.return_value = [(1, "test")]

        mockManager = mocker.MagicMock()
        mockManager.fetchCover.return_value = False

        mockGenerator = mocker.patch("processes.file.covers.CoverManager")
        mockGenerator.return_value = mockManager

        testManager = testProcess.search_for_cover("testEdition")

        assert testManager == None
        mockIdentifierGet.assert_called_once_with("testEdition")
        mockGenerator.assert_called_once_with(
            [(1, "test")], testProcess.db_manager.session
        )
        mockManager.fetch_cover.assert_called_once()
        mockManager.fetch_cover_file.assert_not_called()
        mockManager.resize_cover_file.assert_not_called()

    def test_get_edition_identifiers(self, testProcess, mocker):
        testProcess.redis_manager.check_or_set_key.side_effect = [True, False]

        mockIdentifiers = [
            mocker.MagicMock(identifier=1, authority="test"),
            mocker.MagicMock(identifier=2, authority="test"),
        ]
        mockEdition = mocker.MagicMock(identifiers=mockIdentifiers)

        testIdentifiers = list(testProcess.get_edition_identifiers(mockEdition))

        assert testIdentifiers == [(2, "test")]
        testProcess.redis_manager.check_or_set_key.assert_has_calls(
            [
                mocker.call("sfrCovers", 1, "test", expiration_time=2592000),
                mocker.call("sfrCovers", 2, "test", expiration_time=2592000),
            ]
        )

    def test_store_cover(self, testProcess: CoverProcess, mocker):
        mockFetcher = mocker.MagicMock(SOURCE="test", coverID=1)
        mockManager = mocker.MagicMock(
            fetcher=mockFetcher, coverFormat="tst", coverContent="testBytes"
        )
        mockEdition = mocker.MagicMock(links=[])

        testProcess.editions_to_update = set([f"ed{i}" for i in range(25)])
        testProcess.store_cover(mockManager, mockEdition)

        assert (
            mockEdition.links[0].url
            == "test_aws_bucket.s3.amazonaws.com/covers/test/1.tst"
        )
        assert mockEdition.links[0].media_type == "image/tst"
        assert mockEdition.links[0].flags == {"cover": True}
        testProcess.db_manager.bulk_save_objects.assert_called_once_with(
            testProcess.editions_to_update
        )
        testProcess.s3_manager.put_object.assert_called_once_with(
            "testBytes", "covers/test/1.tst", "test_aws_bucket"
        )
