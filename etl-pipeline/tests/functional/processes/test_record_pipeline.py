from unittest.mock import patch, MagicMock
import pytest
from processes.record_pipeline import RecordPipelineProcess
from model import Record

@pytest.fixture
def mock_managers():
    """Fixture to mock all manager dependencies"""
    with patch.multiple(
        'processes.record_pipeline',
        DBManager=MagicMock(),
        SQSManager=MagicMock(),
        S3Manager=MagicMock(),
        ElasticsearchManager=MagicMock(),
        RedisManager=MagicMock()
    ):
        yield

@pytest.fixture
def mock_processors():
    """Fixture to mock all processor classes"""
    with patch.multiple(
        'processes.record_pipeline',
        RecordFileSaver=MagicMock(),
        RecordEmbellisher=MagicMock(),
        RecordClusterer=MagicMock(),
        LinkFulfiller=MagicMock(),
        RecordDeleter=MagicMock()
    ):
        yield

def test_process_record_success(mock_managers, mock_processors):
    """Test successful record processing"""
    # Setup pipeline with mocks
    pipeline = RecordPipelineProcess()
    
    # Configure mock record
    mock_record = MagicMock(spec=Record)
    mock_record.uuid = "test-uuid-123"
    
    # Configure mock database response
    (pipeline.db_manager.session.query.return_value
     .filter.return_value
     .first.return_value) = mock_record
    
    # Configure processor mocks
    pipeline.record_file_saver.save_record_files.return_value = mock_record
    pipeline.record_embellisher.embellish_record.return_value = mock_record
    pipeline.record_clusterer.cluster_record.return_value = [mock_record]
    
    # Test the method
    result = pipeline.process_record("test-uuid-123")
    
    # Verify results
    assert result is True
    pipeline.db_manager.session.query.assert_called_once_with(Record)
    pipeline.record_file_saver.save_record_files.assert_called_once_with(mock_record)
    pipeline.record_embellisher.embellish_record.assert_called_once_with(mock_record)
    pipeline.record_clusterer.cluster_record.assert_called_once_with(mock_record)
    pipeline.link_fulfiller.fulfill_records_links.assert_called_once_with([mock_record])

def test_process_record_not_found(mock_managers, mock_processors):
    """Test record not found case"""
    pipeline = RecordPipelineProcess()
    
    # Configure mock to return None (record not found)
    (pipeline.db_manager.session.query.return_value
     .filter.return_value
     .first.return_value) = None
    
    result = pipeline.process_record("missing-uuid")
    assert result is False
    pipeline.db_manager.session.query.assert_called_once_with(Record)

def test_process_record_exception(mock_managers, mock_processors):
    """Test exception handling during processing"""
    pipeline = RecordPipelineProcess()
    
    # Configure mock record
    mock_record = MagicMock(spec=Record)
    mock_record.uuid = "test-uuid-123"
    
    # Configure mock database response
    (pipeline.db_manager.session.query.return_value
     .filter.return_value
     .first.return_value) = mock_record
    
    # Make file saver raise an exception
    pipeline.record_file_saver.save_record_files.side_effect = Exception("Test error")
    
    result = pipeline.process_record("test-uuid-123")
    assert result is False