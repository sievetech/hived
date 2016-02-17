from datetime import datetime
import time
import uuid

import amqp
from amqp import Message, AMQPError, ConnectionError as AMQPConnectionError
import simplejson as json

from hived import conf
from hived import process
from hived import trail


MAX_TRIES = 3
META_FIELD = '_meta'
TRAIL_FIELD = '_trail'
STEP_FIELD = '_step'


class ConnectionError(AMQPConnectionError):
    def __str__(self):
        return '%s' % self.message


class SerializationError(Exception):
    def __init__(self, exc, body=None):
        self.exc = exc
        self.body = body

    def __repr__(self):
        return '%s: %s' % (self.exc, repr(self.body))


def add_trail_keys(message, exchange, routing_key):
    message[TRAIL_FIELD] = trail.get_trail()
    message[TRAIL_FIELD]['steps'].append(trail.generate_step_id())
    message[STEP_FIELD] = {'exchange': exchange,
                           'routing_key': routing_key,
                           'address': trail.get_address(),
                           'process': process.get_name(),
                           'time': datetime.now().isoformat()}
    return message  # Makes it easier to test


class ExternalQueue(object):
    """
    For getting messages from the queue, see get(). For publishing, see put().
    The connection is lazy, i.e. it only happens on the first get() / put().

    It also works as a context manager:
    with ExternalQueue(**options) as queue:
        for msg in msgs:
            queue.put(msg)
    """
    def __init__(self, host=conf.QUEUE_HOST, username=conf.QUEUE_USER, password=conf.QUEUE_PASSWORD, virtual_host='/',
                 exchange=None, queue_name=None, queue_heartbeat=None):
        self.default_exchange = exchange
        self.default_queue_name = queue_name
        self.channel = None
        self.subscription = None
        self.connection = None

        self.connection_parameters = {
            'host': host,
            'userid': username,
            'password': password,
            'virtual_host': virtual_host,
            'heartbeat': queue_heartbeat
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
        return False

    def _connect(self):
        try:
            self.close()
        except Exception:
            pass
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
        except (AMQPError, IOError):
            if _tries < MAX_TRIES:
                self._connect()
                return self._try(method, _tries + 1, **kwargs)
            else:
                raise

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

    def put(self, message_dict=None, routing_key='', exchange=None, body=None, priority=0):
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
                message_dict = add_trail_keys(message_dict, exchange, routing_key)
                body = json.dumps(message_dict)
            except Exception as e:
                raise SerializationError(e)

        message = Message(body,
                          delivery_mode=2,
                          content_type='application/json',
                          priority=priority or trail.get_priority())
        return self._try('basic_publish',
                         msg=message,
                         exchange=exchange,
                         routing_key=routing_key)

    def _parse_message(self, message):
        body = message.body
        delivery_tag = message.delivery_info['delivery_tag']
        try:
            message_dict = json.loads(body)
            message_dict.setdefault(META_FIELD, {})
        except Exception:
            self.ack(delivery_tag)
            return

        trail.set_trail(**message_dict.get(TRAIL_FIELD, {}))
        trail.trace(type_=trail.EventType.process_entered)
        return message_dict, delivery_tag

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
        while True:
            message = self._try('basic_get', queue=queue_name or self.default_queue_name)
            if message:
                parsed = self._parse_message(message)
                if parsed:
                    return parsed

            if block:
                time.sleep(.5)
            else:
                return None, None

    def setup_consumer(self, callback, queue_names=None):
        def message_callback(message):
            parsed = self._parse_message(message)
            if parsed:
                callback(*parsed)

        self._try('basic_qos', prefetch_size=0, prefetch_count=1, a_global=False)
        queue_names = queue_names or [self.default_queue_name]
        for queue_name in queue_names:
            self.channel.basic_consume(queue_name, callback=message_callback)

    def consume(self):
        self.connection.drain_events()

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
