import time
import uuid

import amqp
from amqp import AMQPError
import simplejson as json


MAX_TRIES = 3
META_FIELD = '_meta'


class ConnectionError(Exception):
    pass


class SerializationError(Exception):
    def __init__(self, exc, body=None):
        self.exc = exc
        self.body = body

    def __repr__(self):
        return '%s: %s' % (self.exc, repr(self.body))


class ExternalQueue(object):
    def __init__(self, host, username, password,
                 virtual_host='/', exchange=None, queue_name=None):
        self.default_exchange = exchange
        self.default_queue_name = queue_name
        self.channel = None
        self.subscription = None

        self.connection_parameters = {
            'host': host,
            'userid': username,
            'password': password,
            'virtual_host': virtual_host
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _connect(self):
        self.connection = amqp.Connection(**self.connection_parameters)
        self.channel = self.connection.channel()
        if self.subscription:
            self._subscribe()

    def close(self):
        if self.connection is not None:
            self.connection.close()

    def _try(self, method, _tries=1, **kwargs):
        if self.channel is None:
            self._connect()

        try:
            return getattr(self.channel, method)(**kwargs)
        except (AMQPError, IOError) as e:
            if _tries < MAX_TRIES:
                self._connect()
                return self._try(method, _tries + 1, **kwargs)
            else:
                raise ConnectionError(e)

    def _subscribe(self):
        self.default_queue_name = '%s_%s' % (self.subscription, uuid.uuid4())
        self.channel.queue_declare(queue=self.default_queue_name,
                                   durable=False,
                                   exclusive=True,
                                   auto_delete=True)
        self.channel.queue_bind(exchange='notifications',
                                queue=self.default_queue_name,
                                routing_key=self.subscription)

    def subscribe(self, routing_key):
        self.subscription = routing_key
        self._connect()

    def put(self, message_dict=None, routing_key='', exchange=None, body=None):
        if exchange is None:
            exchange = self.default_exchange or ''

        if body is None:
            try:
                body = json.dumps(message_dict)
            except Exception as e:
                raise SerializationError(e)

        message = amqp.basic_message.Message(body,
                                             delivery_mode=2,
                                             content_type='application/json')
        return self._try('basic_publish',
                         msg=message,
                         exchange=exchange,
                         routing_key=routing_key)

    def get(self, queue_name=None, block=True):
        queue_name = queue_name or self.default_queue_name
        while True:
            message = self._try('basic_get', queue=queue_name)
            if message:
                body = message.body
                ack = message.delivery_info['delivery_tag']
                try:
                    message_dict = json.loads(body)
                    message_dict.setdefault(META_FIELD, {})
                except Exception as e:
                    self.ack(ack)
                    raise SerializationError(e, body)

                return message_dict, ack

            if block:
                time.sleep(.5)
            else:
                return None, None

    def ack(self, delivery_tag):
        try:
            self.channel.basic_ack(delivery_tag)
        except AMQPError:
            # There's nothing we can do, we can't ack the message in
            # a different channel than the one we got it from
            pass

    def reject(self, delivery_tag):
        try:
            self.channel.basic_reject(delivery_tag, requeue=True)
        except AMQPError:
            pass  # It's out of our hands already
