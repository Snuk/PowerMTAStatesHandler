import pika
import json


class RabbitSettings:
    def __init__(self, host, port, login, password, queueName):
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.queueName = queueName


class RabbitQueue:
    """
    RabbitMQ provider adapter for simple publish to one queue
    """

    def __init__(self, settings):
        assert isinstance(settings, RabbitSettings)
        self.settings = settings
        self.connection = None
        self.channel = None
        # to read from C# EasyNetQ RabbitMQ driver
        self.properties = pika.BasicProperties(type='System.String:mscorlib')

    def __enter__(self):
        credentials = pika.PlainCredentials(self.settings.login, self.settings.password)
        parameters = pika.ConnectionParameters(self.settings.host, self.settings.port, '/', credentials)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.settings.queueName, durable=True, exclusive=False, auto_delete=False)
        return self

    def __exit__(self, type, value, tb):
        self.connection.close()

    def publish(self, message):
        messageJson = '"' + json.dumps(message).replace('"', '\\"') + '"'
        self.channel.basic_publish('', self.settings.queueName, messageJson, self.properties)
