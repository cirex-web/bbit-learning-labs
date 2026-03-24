from producer_interface import mqProducerInterface
import pika
import os

class mqProducer(mqProducerInterface):
	def __init__(self, routing_key, exchange_name):
		self.routing_key = routing_key
		self.exchange_name = exchange_name
		self.channel = self.setupRMQConnection()

	def setupRMQConnection(self):
		con_params = pika.URLParameters(os.environ["AMQP_URL"])
		connection = pika.BlockingConnection(parameters=con_params)
		channel = connection.channel()
		channel.exchange_declare(exchange=self.exchange_name,exchange_type='topic')

		return channel
		
		# return super().setupRMQConnection()
	
	def publishOrder(self, message):
		self.channel.basic_publish(
			exchange=self.exchange_name,
			routing_key=self.routing_key,
			body=message,
		)
		return super().publishOrder(message)

	