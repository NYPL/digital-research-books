import json
from pika import BlockingConnection, ConnectionParameters
from pika.credentials import PlainCredentials
from pika.exceptions import ConnectionWrongStateError, StreamLostError, ChannelClosedByBroker, ChannelWrongStateError
from typing import Union
import os

from logger import create_log

logger = create_log(__name__)


class RabbitMQManager:
    def __init__(
        self, 
        host=None, 
        port=None, 
        virtual_host=None,
        exchange=None, 
        user=None, 
        pswd=None,
        queue_name=None, 
        routing_key=None,
    ):
        super(RabbitMQManager, self).__init__()
        self.host = host or os.environ.get('RABBIT_HOST', None)
        self.port = port or os.environ.get('RABBIT_PORT', None)
        self.virtual_host = virtual_host or os.environ.get('RABBIT_VIRTUAL_HOST', None)
        self.exchange = exchange or os.environ.get('RABBIT_EXCHANGE', None)

        self.username = user or os.environ.get('RABBIT_USER', None)
        self.password = pswd or os.environ.get('RABBIT_PSWD', None)

        self.queue_name = queue_name
        self.routing_key = routing_key

    def __enter__(self):
        self.create_connection()
        self.create_or_connect_queue(queue_name=self.queue_name, routing_key=self.routing_key)

        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close_connection()
    
    def create_connection(self):
        paramDict = {
            'host': self.host, 'port': self.port, 'heartbeat': 600
        }

        if self.virtual_host:
            paramDict['virtual_host'] = self.virtual_host

        if self.username and self.password:
            paramDict['credentials'] = self.create_credentials() 

        params = ConnectionParameters(**paramDict)

        self.connection = BlockingConnection(params)

    def create_credentials(self) -> PlainCredentials:
        return PlainCredentials(self.username, self.password)
        
    def close_connection(self):
        self.connection.close()
    
    def create_channel(self):
        self.channel = self.connection.channel()
    
    def create_or_connect_queue(self, queue_name: str, routing_key: str):
        self.create_channel()
        self.channel.queue_declare(queue=queue_name, durable=True)

        if self.exchange:
            self.channel.queue_bind(
                exchange=self.exchange,
                queue=queue_name,
                routing_key=routing_key
            )
    
    def send_message_to_queue(self, queue_name: str, routing_key: str, message: Union[str, dict]):
        if isinstance(message, dict):
            message = json.dumps(message)

        try:
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=routing_key,
                body=message
            )
        except (ConnectionWrongStateError, StreamLostError, ChannelWrongStateError):
            # Connection timed out. Reconnect and try again
            self.create_connection()
            self.create_or_connect_queue(queue_name, routing_key)
            self.send_message_to_queue(queue_name, routing_key, message)
    
    def get_message_from_queue(self, queue_name: str):
        try:
            return self.channel.basic_get(queue_name)
        except (ConnectionWrongStateError, StreamLostError):
            self.create_connection()
            self.create_channel()
            return self.get_message_from_queue(queue_name)
        except ChannelClosedByBroker:
            self.create_connection()
            self.create_channel()

        return None
    
    def acknowledge_message_processed(self, delivery_tag: int):
        try:
            self.channel.basic_ack(delivery_tag)
        except (ConnectionWrongStateError, StreamLostError):
            self.create_connection()
            self.create_channel()
            self.acknowledge_message_processed(delivery_tag)

    def reject_message(self, delivery_tag: int, requeue=False):
        try:
            self.channel.basic_reject(delivery_tag=delivery_tag, requeue=requeue)
        except (ConnectionWrongStateError, StreamLostError):
            self.create_connection()
            self.create_channel()
            self.reject_message(delivery_tag, requeue)      
