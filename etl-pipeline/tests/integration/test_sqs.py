import os
import json
import pytest
from managers import SQSManager


@pytest.fixture
def sqs_manager(monkeypatch):
    """Fixture providing an SQSManager instance configured for LocalStack"""
    monkeypatch.setenv('AWS_REGION','us-east-1')
    monkeypatch.setenv('RECORD_PIPELINE_SQS_QUEUE', 'test-queue')
    monkeypatch.setenv('AWS_ACCESS', 'test-access-key')
    monkeypatch.setenv('AWS_SECRET', 'test-secret-key')
    monkeypatch.setenv('S3_ENDPOINT_URL', 'http://localhost:4566')
    manager = SQSManager()
    return manager

class TestSQSManagerIntegration:
    def test_send_and_receive_message(self, sqs_manager):
        """Test full message lifecycle: send -> receive -> delete"""
        # Send test message
        test_msg = {'key': 'value'}
        send_result = sqs_manager.send_message_to_queue(test_msg)
        assert 'MessageId' in send_result

        # Receive message
        message = sqs_manager.get_message_from_queue()
        assert json.loads(message['Body']) == test_msg

        # Clean up
        deleted = sqs_manager.acknowledge_message_processed(message['ReceiptHandle'])
        assert deleted is True
