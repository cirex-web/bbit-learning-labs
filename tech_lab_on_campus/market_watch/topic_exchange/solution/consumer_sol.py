# Class mqConsumer
import json
import pika
import os
from consumer_interface import mqConsumerInterface  # pylint: disable=import-error

class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        # Save parameters to class variables
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        
        self.channel = None

        # Call setupRMQConnection
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        # self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

        # Establish Channel
        self.channel = self.connection.channel()

        # Create the exchange if not already present
        self.channel.exchange_declare(
            exchange="Exchange Name", exchange_type="topic"
        )

    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue=queueName,
            routing_key=self.binding_key,
            exchange=self.exchange_name,
        )

    def createQueue(self, queueName: str) -> None:
        # Create Queue if not already present
        self.channel.queue_declare(queue=queueName)

        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            queueName, self.on_message_callback, auto_ack=False
        )

    def on_message_callback(self, channel, method_frame, header_frame, body):
        # De-Serialize JSON message object if Stock Object Sent
        message = json.loads(body)

        # Acknowledge And Print Message
        channel.basic_ack(method_frame.delivery_tag, False)
        print(message)

    def startConsuming(self) -> None:
        # Start consuming messages
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()