from datetime import datetime
import os
import pytest
from unittest.mock import ANY

from tests.helper import TestHelpers
from processes import ClassifyProcess
from processes.utils import ProcessParams

class TestOCLCClassifyProcess:
    @classmethod
    def setup_class(cls):
        TestHelpers.setEnvVars()

    @classmethod
    def teardown_class(cls):
        TestHelpers.clearEnvVars()

    @pytest.fixture
    def test_instance(self, mocker):
        class TestClassifyProcess(ClassifyProcess):
            def __init__(self, *args):
                self.params = ProcessParams()
                self.records = set()
                self.limit = None
                self.ingest_period = None
                self.single_record = None
                self.db_manager = mocker.MagicMock()
                self.record_buffer = mocker.MagicMock()
                self.source = None
                self.catalog_queue = os.environ['OCLC_QUEUE']
                self.catalog_route = os.environ['OCLC_ROUTING_KEY']
                self.classified_count = 0
                self.oclc_catalog_manager = mocker.MagicMock()
                self.rabbitmq_manager = mocker.MagicMock()
                self.redis_manager = mocker.MagicMock()

        return TestClassifyProcess('TestProcess', 'testFile', 'testDate')

    @pytest.fixture
    def run_process_mocks(self, mocker):
        return mocker.patch.multiple(
            ClassifyProcess,
            classify_records=mocker.DEFAULT,
        )

    @pytest.fixture
    def test_record(self, mocker):
        mockRecord = mocker.MagicMock(name='test_record')
        mockRecord.identifiers = ['1|test']
        mockRecord.authors = ['Author, Test|||true']
        mockRecord.title = 'Test Record'

        return mockRecord

    def test_runProcess_daily(self, test_instance: ClassifyProcess, run_process_mocks):
        test_instance.params.process_type = 'daily'
        test_instance.runProcess()

        for _, mockedMethod in run_process_mocks.items():
            mockedMethod.assert_called_once()

    def test_runProcess_complete(self, test_instance: ClassifyProcess, run_process_mocks):
        test_instance.params.process_type = 'complete'
        test_instance.runProcess()

        run_process_mocks['classify_records'].assert_called_once_with(start_datetime=None, record_uuid=None, source=None)
        for _, mocked_method in run_process_mocks.items():
            mocked_method.assert_called_once()

    def test_runProcess_custom(self, test_instance: ClassifyProcess, run_process_mocks):
        test_instance.params.process_type = 'custom'
        test_instance.params.ingest_period = '2022-01-01T00:00:00'
        test_instance.runProcess()

        run_process_mocks['classify_records'].assert_called_once_with(start_datetime=datetime.strptime(test_instance.params.ingest_period, '%Y-%m-%dT%H:%M:%S'), record_uuid=None, source=None)
        
        for _, mocked_method in run_process_mocks.items():
            mocked_method.assert_called_once()

    def test_classify_records_not_full(self, test_instance, mocker):
        mock_embellish_record = mocker.patch.object(ClassifyProcess, 'embellish_record')
        mock_query = mocker.MagicMock()

        test_instance.db_manager.session.query().filter.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_records = [mocker.MagicMock(name=i) for i in range(100)]
        mock_query.first.side_effect = mock_records + [None]

        test_instance.redis_manager.check_incrementer.side_effect = [False] * 100

        test_instance.classify_records()

        mock_embellish_record.assert_has_calls([mocker.call(record) for record in mock_records])

    def test_classify_records_custom_range(self, test_instance, mocker):
        mock_embellish_record = mocker.patch.object(ClassifyProcess, 'embellish_record')
        mock_query = mocker.MagicMock()

        test_instance.db_manager.session.query().filter.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_records = [mocker.MagicMock(name=i) for i in range(100)]
        mock_query.first.side_effect = mock_records + [None]

        test_instance.redis_manager.check_incrementer.side_effect = [False] * 100

        test_instance.classify_records(start_datetime=datetime.now())

        mock_embellish_record.assert_has_calls([mocker.call(record) for record in mock_records])

    def test_classify_records_full(self, test_instance, mocker):
        mock_embellish_record = mocker.patch.object(ClassifyProcess, 'embellish_record')
        mock_query = mocker.MagicMock()

        test_instance.db_manager.session.query().filter.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_records = [mocker.MagicMock(name=i) for i in range(100)]
        mock_query.first.side_effect = mock_records + [None]

        test_instance.redis_manager.check_incrementer.side_effect = [False] * 50 + [True]

        test_instance.classify_records()

        mock_embellish_record.assert_has_calls([mocker.call(record) for record in mock_records[:50]])

    def test_classify_records_full_batch(self, test_instance, mocker):
        mock_embellish_record = mocker.patch.object(ClassifyProcess, 'embellish_record')
        mock_query = mocker.MagicMock()

        test_instance.db_manager.session.query().filter.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_records = [mocker.MagicMock(name=i) for i in range(100)]
        mock_query.first.side_effect = mock_records + [None]

        test_instance.redis_manager.check_incrementer.side_effect = [False] * 100

        test_instance.ingest_limit = 100
        test_instance.classify_records()

        mock_embellish_record.assert_has_calls([mocker.call(record) for record in mock_records])

    def test_embellish_record_success_valid_author(self, test_instance, test_record, mocker):
        mock_identifiers = mocker.patch.object(ClassifyProcess, '_get_queryable_identifiers')
        mock_identifiers.return_value = ['1|test']

        test_instance.redis_manager.check_or_set_key.return_value = False

        mock_classify_record = mocker.patch.object(ClassifyProcess, 'classify_record_by_metadata')

        test_instance.embellish_record(test_record)

        mock_identifiers.assert_called_once_with(test_record.identifiers)
        test_instance.redis_manager.check_or_set_key.assert_called_once_with('classify', '1', 'test')
        mock_classify_record.assert_called_once_with('1', 'test', 'Author, Test', 'Test Record')

    def test_embellish_record_success_author_missing(self, test_instance, test_record, mocker):
        mock_identifiers = mocker.patch.object(ClassifyProcess, '_get_queryable_identifiers')
        mock_identifiers.return_value = ['1|test']

        test_instance.redis_manager.check_or_set_key.return_value = False

        mock_classify_record = mocker.patch.object(ClassifyProcess, 'classify_record_by_metadata')

        test_record.authors = []
        test_instance.embellish_record(test_record)

        mock_identifiers.assert_called_once_with(test_record.identifiers)
        test_instance.redis_manager.check_or_set_key.assert_called_once_with('classify', '1', 'test')
        mock_classify_record.assert_called_once_with('1', 'test', None, 'Test Record')

    def test_embellish_record_identifier_in_redis(self, test_instance, test_record, mocker):
        mock_identifiers = mocker.patch.object(ClassifyProcess, '_get_queryable_identifiers')
        mock_identifiers.return_value = ['1|test']

        test_instance.redis_manager.check_or_set_key.return_value = True

        mock_classify_record = mocker.patch.object(ClassifyProcess, 'classify_record_by_metadata')

        test_record.authors = []
        test_instance.embellish_record(test_record)

        mock_identifiers.assert_called_once_with(test_record.identifiers)
        test_instance.redis_manager.check_or_set_key.assert_called_once_with('classify', '1', 'test')
        mock_classify_record.assert_not_called

    def test_embellish_record_identifier_missing(self, test_instance, test_record, mocker):
        mock_identifiers = mocker.patch.object(ClassifyProcess, '_get_queryable_identifiers')
        mock_identifiers.return_value = []
        mock_classify_record = mocker.patch.object(ClassifyProcess, 'classify_record_by_metadata')

        test_instance.embellish_record(test_record)

        mock_identifiers.assert_called_once_with(test_record.identifiers)
        test_instance.redis_manager.check_or_set_key.assert_not_called()
        mock_classify_record.assert_called_once_with(None, None, 'Author, Test', 'Test Record')

    def test_get_oclc_catalog_records_no_redis_match(self, test_instance, mocker):
        test_instance.redis_manager.multi_check_or_set_key.return_value = [('2', True)]

        test_instance.get_oclc_catalog_records(['1|owi', '2|oclc'])

        test_instance.redis_manager.multi_check_or_set_key.assert_called_once_with('catalog', ['2'], 'oclc')
        test_instance.rabbitmq_manager.send_message_to_queue.assert_called_once_with('test_oclc_queue', 'test_oclc_key', {'oclcNo': '2', 'owiNo': '1'})
        test_instance.redis_manager.set_incrementer.assert_called_once_with('oclcCatalog', 'API', amount=1)

    def test_get_oclc_catalog_records_redis_match(self, test_instance, mocker):
        test_instance.redis_manager.multi_check_or_set_key.return_value = [(2, False)]

        test_instance.get_oclc_catalog_records(['1|test', '2|oclc'])

        test_instance.redis_manager.multi_check_or_set_key.assert_called_once_with('catalog', ['2'], 'oclc')
        test_instance.rabbitmq_manager.send_message_to_queue.assert_not_called()
        test_instance.redis_manager.set_incrementer.assert_not_called()
    
    def test_get_queryable_identifiers(self, test_instance):
        assert test_instance._get_queryable_identifiers(['1|isbn', '2|test']) == ['1|isbn']
