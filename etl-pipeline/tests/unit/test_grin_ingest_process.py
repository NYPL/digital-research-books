import unittest
import pytest
from managers import SQSManager
from processes.grin_ingest_process import GRINIngestProcess

@pytest.fixture
def sqs_manager(monkeypatch):
    """Fixture providing an SQSManager instance configured for LocalStack"""
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS", "test-access-key")
    monkeypatch.setenv("AWS_SECRET", "test-secret-key")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:4566")
    manager = SQSManager("test-queue")
    return manager

def test_parse_message(sqs_manager):
    test_msg = {"barcode": "1234"}
    sqs_manager.send_message_to_queue(test_msg)
    
    sqs_message = sqs_manager.get_messages_from_queue()

    grin = GRINIngestProcess()
    barcode, receipt_handle = grin._parse_message(sqs_message[0])

    assert barcode == "1234"