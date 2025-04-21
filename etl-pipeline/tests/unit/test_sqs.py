import pytest
import os
from unittest.mock import MagicMock, patch, call
from botocore.exceptions import ClientError
from managers.sqs import SQSManager
from managers.rabbitmq import RabbitMQManager
from pika.exceptions import StreamLostError
from pika import BlockingConnection


class TestSQSManager:
    @pytest.fixture(autouse=True)
    def mock_client(self):
        with patch("managers.sqs.boto3.client") as mock_client:
            mock_sqs = MagicMock()
            mock_client.return_value = mock_sqs
            yield mock_sqs

    @pytest.fixture
    def manager(self, mock_client, monkeypatch):
        # Set up AWS environment variables
        monkeypatch.setenv("AWS_REGION", "env-region")
        monkeypatch.setenv("S3_ENDPOINT_URL", "http://test-endpoint")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
        monkeypatch.setenv("RECORD_PIPELINE_SQS_QUEUE", "test-queue")
        monkeypatch.setenv("RECORD_PIPELINE_ROUTING_KEY", "test-route")

        # Set up RabbitMQ environment variables
        monkeypatch.setenv("RABBIT_HOST", "localhost")
        monkeypatch.setenv("RABBIT_PORT", "5672")
        monkeypatch.setenv("RABBIT_VIRTUAL_HOST", "/")
        monkeypatch.setenv("RABBIT_EXCHANGE", "test-exchange")
        monkeypatch.setenv("RABBIT_USER", "guest")
        monkeypatch.setenv("RABBIT_PSWD", "guest")

        manager = SQSManager()
        manager.create_client()
        assert manager.region_name == "env-region"
        assert manager.endpoint_url == "http://test-endpoint"
        assert manager.aws_access_key_id == "env-key"
        assert manager.aws_secret_access_key == "env-secret"
        assert manager.client == mock_client

        yield manager

    @pytest.fixture
    def mock_rabbit(self, mocker):
        # Create RabbitMQ manager with explicit parameters
        rabbit = RabbitMQManager(
            host="localhost",
            port=5672,
            virtual_host="/",
            exchange="test_exchange",
            user="guest",
            pswd="guest",
        )

        # Mock the connection and channel
        rabbit.connection = MagicMock()
        rabbit.channel = MagicMock()

        return rabbit

    def test_get_queue(self, manager, mock_client):
        mock_client.get_queue_by_queue_name.return_value = {
            "QueueUrl": "existing-queue-url"
        }
        response = manager.get_queue()
        assert manager.queue_url == response["QueueUrl"]

    def test_send_message(self, manager):
        with patch("managers.sqs.SQSManager.get_queue") as mock_get_queue:
            queue = MagicMock()
            queue.url = "test-queue-url"
            queue.send_message.return_value = {"MessageId": "test-id"}
            mock_get_queue.return_value = queue

            message = manager.send_message_to_queue({"key": "value"})
            assert message == {"MessageId": "test-id"}
            queue.send_message.assert_called_once_with(
                MessageAttributes='{"key": "value"}'
            )

    def test_send_message_retry(self, manager):
        with patch("managers.sqs.SQSManager.get_queue") as mock_get_queue:
            mock_queue = MagicMock()
            mock_queue.url = "test-queue-url"
            mock_queue.send_message.side_effect = [
                ClientError({"Error": {"Code": "500"}}, "SendMessage"),
                {"MessageId": "retry-id"},
            ]
            mock_get_queue.return_value = mock_queue

            message = manager.send_message_to_queue({"key": "value"})
            assert mock_queue.send_message.call_count == 2
            assert message == {"MessageId": "retry-id"}

    def test_get_message_from_rabbitmq(self, manager, mock_rabbit):
        """Test successful message retrieval from RabbitMQ"""
        # Fully mock the RabbitMQManager to prevent real connections
        with patch('managers.sqs.RabbitMQManager') as MockRabbit:
            mock_rabbit = MockRabbit.return_value
            manager.rabbit_mq = mock_rabbit
            
            # Setup mock return value
            mock_message = MagicMock()
            mock_message.body = '{"test": "value"}'
            mock_message.delivery_tag = 1234
            mock_rabbit.get_message_from_queue.return_value = (mock_message, None)
            
            # Mock connection methods to do nothing
            mock_rabbit.create_connection.return_value = None
            mock_rabbit.create_or_connect_queue.return_value = None
            
            result = manager.get_message_from_queue()
            
            # Verify calls
            mock_rabbit.create_connection.assert_called_once()
            mock_rabbit.create_or_connect_queue.assert_called_once_with(
                manager.queue_name,
                manager.rabbit_route
            )
            mock_rabbit.get_message_from_queue.assert_called_once_with(
                manager.rabbit_route
            )
            assert result[0].body == '{"test": "value"}'
            assert result[1] is None

    def test_fallback_to_sqs_when_no_rabbit_message(self, manager):
        """Test fallback to SQS when RabbitMQ returns None"""
        with patch('managers.sqs.RabbitMQManager') as MockRabbit, \
             patch("managers.sqs.SQSManager.get_queue") as mock_get_queue:
            
            # Setup mock RabbitMQ that returns None
            mock_rabbit = MockRabbit.return_value
            mock_rabbit.get_message_from_queue.return_value = None
            manager.rabbit_mq = mock_rabbit
            
            # Setup mock SQS message
            mock_queue = MagicMock()
            mock_message = MagicMock()
            mock_message.message_attributes = {"test": "value"}
            mock_queue.receive_messages.return_value = [mock_message]
            mock_get_queue.return_value = mock_queue
            
            result = manager.get_message_from_queue()
            
            # Verify RabbitMQ was tried first
            mock_rabbit.get_message_from_queue.assert_called_once_with(
                manager.rabbit_route
            )
            
            # Verify SQS fallback
            mock_queue.receive_messages.assert_called_once_with(
                MessageAttributeNames=["All"], 
                MaxNumberOfMessages=1
            )
            
            # Verify return format matches SQS expectation
            assert isinstance(result, tuple)
            assert len(result) == 3  # (message, None, None)
            assert result[0].message_attributes == {"test": "value"}

    def test_fallback_to_sqs_on_rabbit_error(self, manager):
        """Test fallback to SQS when RabbitMQ connection fails"""
        with patch('managers.sqs.RabbitMQManager') as MockRabbit, \
             patch("managers.sqs.SQSManager.get_queue") as mock_get_queue:
            
            # Setup mock RabbitMQ that raises connection error
            mock_rabbit = MockRabbit.return_value
            mock_rabbit.get_message_from_queue.side_effect = StreamLostError()
            mock_rabbit.create_connection.side_effect = StreamLostError()
            manager.rabbit_mq = mock_rabbit
            
            # Setup mock SQS message
            mock_queue = MagicMock()
            mock_message = MagicMock()
            mock_message.message_attributes = {"test": "value"}
            mock_queue.receive_messages.return_value = [mock_message]
            mock_get_queue.return_value = mock_queue
            
            result = manager.get_message_from_queue()
            
            # Verify RabbitMQ was tried first
            mock_rabbit.create_connection.assert_called_once()
            mock_rabbit.get_message_from_queue.assert_not_called()
            
            # Verify SQS fallback
            mock_queue.receive_messages.assert_called_once_with(
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=1
            )
            
            # Verify return format matches SQS expectation
            assert isinstance(result, tuple)
            assert len(result) == 3  # (message, None, None)
            assert result[0].message_attributes == {"test": "value"}

    def test_acknowledge_message(self, manager):
        """Test message acknowledgement works for both SQS and RabbitMQ messages"""
        # Test SQS message acknowledgement
        with patch.object(manager, 'get_queue') as mock_get_queue:
            mock_queue = MagicMock()
            mock_queue.url = "test-queue-url"
            mock_get_queue.return_value = mock_queue
            
            mock_message = MagicMock()
            mock_message.receipt_handle = "test-handle"
            
            manager.acknowledge_message_processed(mock_message)
            mock_message.delete.assert_called_once()

        # Test RabbitMQ message acknowledgement
        with patch('managers.sqs.RabbitMQManager') as MockRabbit:
            mock_rabbit = MockRabbit.return_value
            manager.rabbit_mq = mock_rabbit
            
            manager.acknowledge_message_processed(1234)  # Using int for RabbitMQ delivery tag
            
            mock_rabbit.create_connection.assert_called_once()
            mock_rabbit.create_or_connect_queue.assert_called_once_with(
                manager.queue_name,
                manager.rabbit_route
            )
            mock_rabbit.acknowledge_message_processed.assert_called_once_with(1234)

    def test_reject_message_requeue_sqs(self, manager):
        """Test SQS message rejection with requeue"""
        with patch.object(manager, 'get_queue') as mock_get_queue:
            mock_queue = MagicMock()
            mock_queue.url = "test-queue-url"
            mock_get_queue.return_value = mock_queue
            
            mock_message = MagicMock()
            mock_message.receipt_handle = "test-handle"
            
            # Set queue_url on manager since reject_message will use it
            manager.queue_url = "test-queue-url"
            
            manager.reject_message(mock_message, requeue=True)
            # Verify SQS client called with correct visibility timeout
            manager.client.change_message_visibility.assert_called_once_with(
                QueueUrl="test-queue-url",
                ReceiptHandle="test-handle",
                VisibilityTimeout=0
            )
            # Verify no message deletion occurred
            mock_message.delete.assert_not_called()

    def test_reject_message_requeue_rabbitmq(self, manager):
        """Test RabbitMQ message rejection with requeue"""
        with patch('managers.sqs.RabbitMQManager') as MockRabbit:
            mock_rabbit = MockRabbit.return_value
            manager.rabbit_mq = mock_rabbit
            
            # Create a mock to track calls
            mock_reject = MagicMock()
            mock_rabbit.reject_message = mock_reject
            
            manager.reject_message(1234, requeue=True)  # Using int for RabbitMQ delivery tag
            
            mock_rabbit.create_connection.assert_called_once()
            mock_rabbit.create_or_connect_queue.assert_called_once_with(
                manager.queue_name,
                manager.rabbit_route
            )
            # Verify RabbitMQ reject called with requeue=True
            mock_reject.assert_called_once_with(1234, requeue=True)

    def test_reject_message_no_requeue(self, manager, mock_client):
        """Test message rejection without requeue for both SQS and RabbitMQ"""
        # Test SQS message no requeue
        mock_message = MagicMock()
        mock_message.receipt_handle = "test-handle"

        manager.reject_message(mock_message, requeue=False)
        # Verify message was deleted (not requeued)
        mock_message.delete.assert_called_once()
        # Verify no visibility timeout change occurred
        manager.client.change_message_visibility.assert_not_called()

        # Test RabbitMQ message no requeue
        with patch('managers.sqs.RabbitMQManager') as MockRabbit:
            mock_rabbit = MockRabbit.return_value
            manager.rabbit_mq = mock_rabbit
            
            # Create a mock to track calls
            mock_reject = MagicMock()
            mock_rabbit.reject_message = mock_reject
            
            manager.reject_message(1234, requeue=False)  # Using int for RabbitMQ delivery tag
            
            mock_rabbit.create_connection.assert_called_once()
            mock_rabbit.create_or_connect_queue.assert_called_once_with(
                manager.queue_name,
                manager.rabbit_route
            )
            # Verify RabbitMQ reject called with requeue=False
            mock_reject.assert_called_once_with(1234, requeue=False)
