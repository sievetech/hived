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
    """
    For getting messages from the queue, see get(). For publishing, see put().
    The connection is lazy, i.e. it only happens on the first get() / put().

    It also works as a context manager:
    with ExternalQueue(**options) as queue:
        for msg in msgs:
            queue.put(msg)
    """
    def __init__(self, host, username, password,
                 virtual_host='/', exchange=None, queue_name=None, priority=False):
        self.default_exchange = exchange
        self.default_queue_name = queue_name
        self.priority_queue_name = queue_name + '_priority' if priority else queue_name
        self.priority_count = 0
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
        """
        Publishes a message to the queue.
        message_dict: the json-serializable object that will be published
            when body is None
        routing_key: the routing key for the message.
        exchange: the exchange to which the message will be published.
            Defaults to the one passed on __init___().  key
        body: The message to be published. If none, message_dict is published.
        """
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
        """
        Gets messages from the queue.
        queue_name: optional, defaults to the one passed on __init__().
        block: boolean. If block is True (default), get() will not return until
            a message is acquired from the queue.

        Returns a tuple (message, delivery_tag) when a message is read, where
        message is a deserialized json and delivery_tag is a parameter used
        for on ack() and reject() methods. If block is False and there's no
        message on the queue, returns (None, None).
        """

        if queue_name:
            name_list = [queue_name]

        elif self.priority_queue_name:
            name_list = [self.priority_queue_name, self.default_queue_name]
            if self.priority_count in (2, 5, 8):
                # In 3 of 10 cases it picks the default queue first; otherwise picks the priority queue
                name_list = name_list[::-1]
            self.priority_count = (self.priority_count + 1) % 10

        else:
            name_list = [self.default_queue_name]

        aux = None
        while True:
            if not aux:
                aux = name_list[:]

            message = self._try('basic_get', queue=aux.pop(0))
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

            if not aux:
                if block:
                    time.sleep(.5)
                else:
                    return None, None

    def ack(self, delivery_tag):
        """
        Acks a message from the queue.
        delivery_tag: second value on the tuple returned from get().
        """
        try:
            self.channel.basic_ack(delivery_tag)
        except AMQPError:
            # There's nothing we can do, we can't ack the message in
            # a different channel than the one we got it from
            pass

    def reject(self, delivery_tag):
        """
        Rejects a message from the queue, i.e. returns it to the top of the queue.
        delivery_tag: second value on the tuple returned from get().
        """
        try:
            self.channel.basic_reject(delivery_tag, requeue=True)
        except AMQPError:
            pass  # It's out of our hands already

