import json
import boto3
from botocore.exceptions import ClientError
from typing import Union
import os

from logger import create_log

logger = create_log(__name__)


class SQSManager:
    def __init__(
        self,
        queue_name: str,
        visibility_timeout=30,
        max_receive_count=1,
        wait_time_seconds=10,
    ):
        self.region_name = os.environ.get("AWS_REGION", "us-east-1")
        self.endpoint_url = os.environ.get("S3_ENDPOINT_URL")
        self.aws_access_key_id = os.environ.get("AWS_ACCESS")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET")
        self.queue_name = queue_name
        self.visibility_timeout = visibility_timeout
        self.max_receive_count = max_receive_count
        self.wait_time_seconds = wait_time_seconds
        self.queue_url = None
        self.client = self.create_client()

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
        self.queue_url = self.client.get_queue_url(QueueName=self.queue_name)[
            "QueueUrl"
        ]

    def send_message_to_queue(self, message: Union[str, dict]):
        if not self.client:
            self.create_client()
        if isinstance(message, dict):
            message = json.dumps(message)

        params = {
            "QueueUrl": self.queue_url,
            "MessageBody": message,
        }

        try:
            response = self.client.send_message(**params)
            return response
        except ClientError as e:
            logger.error(f"Failed to send message to SQS: {e}")
            # Reconnect and retry once
            self.create_client()
            try:
                response = self.client.send_message(**params)
                return response
            except ClientError as e:
                logger.error(f"Failed retry sending message to SQS: {e}")
                raise

    def get_messages_from_queue(self, visibility_timeout=None):
        if not self.client:
            self.create_client()
        try:
            receive_message_kwargs = {
                "QueueUrl": self.queue_url,
                "WaitTimeSeconds": self.wait_time_seconds,
                "MessageAttributeNames": ["All"],
                "MaxNumberOfMessages": self.max_receive_count,
            }
            if visibility_timeout:
                receive_message_kwargs["VisibilityTimeout"] = visibility_timeout

            response = self.client.receive_message(**receive_message_kwargs)
            return response["Messages"] if "Messages" in response else None
        except ClientError as e:
            logger.error(f"Failed to receive message from SQS: {e}")
            raise

    def acknowledge_message_processed(self, receipt_handle):
        if not self.client:
            self.create_client()
        try:
            self.client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )
            return True
        except ClientError as e:
            logger.error(f"Failed to delete/acknowledge message: {e}")
            raise

    def reject_message(self, receipt_handle, requeue=False):
        if not self.client:
            self.create_client()
        try:
            if requeue:
                # Reset visibility timeout to make immediately available
                self.client.change_message_visibility(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=receipt_handle,
                    VisibilityTimeout=0,
                )
            else:
                self.acknowledge_message_processed(receipt_handle)
        except ClientError as e:
            logger.error(f"Failed to reject message: {e}")
            raise
