import unittest
from unittest.mock import patch, MagicMock
import os
from botocore.exceptions import ClientError

from managers import SQSManager


class TestSQSManager(unittest.TestCase):
    @patch.dict(os.environ, {
        "AWS_ACCESS_KEY_ID": "test-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret",
        "AWS_REGION": "us-east-1",
        "RECORD_PIPELINE_SQS_QUEUE": "test-queue"
    })
    @patch('managers.sqs.boto3.client')
    def setUp(self, mock_client):
        # Create mock client and configure it
        self.mock_sqs = MagicMock()
        self.mock_sqs.get_queue_url.return_value = {'QueueUrl': 'test-url'}
        mock_client.return_value = self.mock_sqs

        # Create manager and fully mock its client initialization
        self.manager = SQSManager()
        self.manager.client = self.mock_sqs
        self.manager.queue_url = 'test-url'
        self.manager.create_client = MagicMock(return_value=None)  # Prevent real client creation

    def test_send_message_success(self):
        test_msg = {"key": "value"}
        self.manager.send_message_to_queue(test_msg)

        self.mock_sqs.send_message.assert_called_once_with(
            QueueUrl='test-url',
            MessageBody='{"key": "value"}'
        )

    def test_get_messages_success(self):
        test_msg = {
            'Body': 'test',
            'ReceiptHandle': 'handle'
        }
        self.mock_sqs.receive_message.return_value = {'Messages': [test_msg]}

        result = self.manager.get_messages_from_queue()
        self.assertEqual(result, [test_msg])

    def test_get_message_empty(self):
        self.mock_sqs.receive_message.return_value = {}
        self.manager.get_messages_from_queue() == []

    def test_send_message_retry(self):
        self.mock_sqs.send_message.side_effect = [
            ClientError({}, "SendMessage"),
            "success_response"
        ]

        response = self.manager.send_message_to_queue({"key": "value"})
        self.assertEqual(response, "success_response")
        self.assertEqual(self.mock_sqs.send_message.call_count, 2)
