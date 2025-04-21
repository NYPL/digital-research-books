import json
import boto3
from botocore.exceptions import ClientError
from typing import Union
import os
from pika.exceptions import StreamLostError

from managers import RabbitMQManager
from logger import create_log

logger = create_log(__name__)


class SQSManager:
    def __init__(self, visibility_timeout=30, max_receive_count=1):
        super(SQSManager, self).__init__()
        self.region_name = os.environ.get("AWS_REGION", "us-east-1")
        self.endpoint_url = os.environ.get("S3_ENDPOINT_URL")
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.queue_name = os.environ.get("RECORD_PIPELINE_SQS_QUEUE", "records")
        self.visibility_timeout = visibility_timeout
        self.max_receive_count = max_receive_count
        self.client = self.create_client()
        self.queue_url = None
        self.rabbit_route = os.environ.get("RECORD_PIPELINE_ROUTING_KEY", 'records')
        self.rabbit_mq = RabbitMQManager()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        pass  # boto3 client doesn't need explicit cleanup

    def create_client(self):
        client_params = {
            "region_name": self.region_name,
        }

        if self.endpoint_url:
            client_params["endpoint_url"] = self.endpoint_url

        if self.aws_access_key_id and self.aws_secret_access_key:
            client_params["aws_access_key_id"] = self.aws_access_key_id
            client_params["aws_secret_access_key"] = self.aws_secret_access_key

        self.client = boto3.client("sqs", **client_params)

    def get_queue(self) -> boto3.resources.base.ServiceResource:  # type: ignore
        """Get the SQS queue resource with type annotations"""
        if not self.client:
            self.create_client()
            
        assert self.client is not None  # Tell type checker client is initialized
        
        try:
            response = self.client.get_queue_by_queue_name(QueueName=self.queue_name)
            self.queue_url = response["QueueUrl"]
            return response
        except ClientError as e:
            logger.error(f"Failed to connect to SQS queue: {e}")
            raise

    def send_message_to_queue(self, message: Union[str, dict]):
        queue = self.get_queue()
        if isinstance(message, dict):
            message = json.dumps(message)

        params = {"MessageAttributes": message}

        try:
            response = queue.send_message(**params)
            return response
        except ClientError as e:
            logger.error(f"Failed to send message to SQS: {e}")
            # Reconnect and retry once
            self.create_client()
            queue = self.get_queue()
            try:
                response = queue.send_message(**params)
                return response
            except ClientError as e:
                logger.error(f"Failed retry sending message to SQS: {e}")
                raise

    def get_message_from_queue(self):
        # Try to get messages from rabbitmq first
        try:
            self.rabbit_mq.create_connection()
            self.rabbit_mq.create_or_connect_queue(self.queue_name, self.rabbit_route)
            message = self.rabbit_mq.get_message_from_queue(self.rabbit_route)
            if message != None:
                return message
        except (StreamLostError, ConnectionError):
            pass  # Fall through to SQS
        
        queue = self.get_queue()
        try:
            response = queue.receive_messages(
                MessageAttributeNames=["All"],
                MaxNumberOfMessages=self.max_receive_count,
            )
            return (response[0], None, None)
        except ClientError as e:
            logger.error(f"Failed to receive message from SQS: {e}")
            raise

    def acknowledge_message_processed(self, message):
        if isinstance(message, int):
            self.rabbit_mq.create_connection()
            self.rabbit_mq.create_or_connect_queue(self.queue_name, self.rabbit_route)
            self.rabbit_mq.acknowledge_message_processed(message)
            return
        try:
            message.delete()
        except ClientError as e:
            logger.error(f"Failed to delete/acknowledge message: {e}")
            raise

    def reject_message(self, message, requeue=False):
        if isinstance(message, int):
            self.rabbit_mq.create_connection()
            self.rabbit_mq.create_or_connect_queue(self.queue_name, self.rabbit_route)
            self.rabbit_mq.reject_message(message, requeue=requeue)
            return
        
        # Ensure client and queue are initialized
        if not self.client:
            self.create_client()
        if not self.queue_url:
            self.get_queue()
            
        try:
            if requeue:
                # Reset visibility timeout to make immediately available
                assert self.client is not None  # Tell type checker client exists
                self.client.change_message_visibility(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=message.receipt_handle,
                    VisibilityTimeout=0,
                )
            else:
                # Let message go to DLQ after max receives
                self.acknowledge_message_processed(message)
        except ClientError as e:
            logger.error(f"Failed to reject message: {e}")
            raise
