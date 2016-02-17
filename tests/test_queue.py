from datetime import datetime
import json
import unittest

from amqp import Message, AMQPError, ConnectionError
from mock import MagicMock, patch, call, Mock, ANY

from hived.queue import (ExternalQueue, MAX_TRIES, SerializationError,
                         META_FIELD, TRAIL_FIELD, STEP_FIELD, add_trail_keys)
from hived import trail


MODULE = 'hived.queue.'


class ExternalQueueTest(unittest.TestCase):
    def setUp(self):
        self.trail = {'id_': 'trail_id', 'live': 'live', 'steps': []}
        self.trail_patcher = patch('hived.trail.get_trail', return_value=self.trail)
        self.trail_patcher.start()

        _delivery_info = {'delivery_tag': 'delivery_tag'}

        self.message = MagicMock()
        self.message.body = json.dumps({TRAIL_FIELD: self.trail})
        self.message.delivery_info = _delivery_info

        self.channel_mock = MagicMock()
        self.channel_mock.basic_get.return_value = self.message

        self.connection = MagicMock()
        self.connection.channel.return_value = self.channel_mock

        self.connection_cls_patcher = patch('amqp.Connection',
                                                 return_value=self.connection)
        self.connection_cls_mock = self.connection_cls_patcher.start()

        self.external_queue = ExternalQueue('localhost', 'username', 'pwd',
                                            exchange='default_exchange',
                                            queue_name='default_queue')

    def tearDown(self):
        self.connection_cls_patcher.stop()
        self.trail_patcher.stop()

    def test_connect_calls_close_before_creating_a_new_connection(self):
        with patch(MODULE + 'ExternalQueue.close') as close_mock:
            self.external_queue._connect()
            self.assertEqual(close_mock.call_count, 1)

    def test_connect_ignores_close_errors(self):
        with patch.object(self.external_queue, 'close', side_effect=[Exception]) as mock_close:
            self.external_queue._connect()
            self.assertRaises
            self.assertEqual(mock_close.call_count, 1)

    def test_connect_subscribes_if_subscription_is_set(self):
        with patch.object(self.external_queue, 'close'), \
             patch.object(self.external_queue, '_subscribe') as mock_subscribe:
            self.external_queue.subscription = 'routing_key'
            self.external_queue._connect()

            self.assertEqual(mock_subscribe.call_count, 1)

    def test__try_connects_if_disconnected(self):
        self.channel_mock.method.return_value = 'rv'
        rv = self.external_queue._try('method', arg='value')

        self.assertEqual(self.connection_cls_mock.call_count, 1)
        self.assertEqual(self.channel_mock.method.call_args_list,
                         [call(arg='value')])
        self.assertEqual(rv, 'rv')

    def test__try_tries_up_to_max_tries(self):
        self.channel_mock.method.side_effect = [AMQPError, AMQPError, 'rv']
        rv = self.external_queue._try('method')

        self.assertEqual(self.channel_mock.method.call_count, MAX_TRIES)
        self.assertEqual(rv, 'rv')

    def test__try_doesnt_try_more_than_max_tries(self):
        self.channel_mock.method.side_effect = [AMQPError, AMQPError, AMQPError, 'rv']
        self.assertRaises(AMQPError, self.external_queue._try, 'method')

    def test_put_uses_default_exchange_if_not_supplied(self):
        amqp_msg = Message('body', delivery_mode=2, content_type='application/json', priority=0)
        self.external_queue.put(body='body')
        self.assertEqual(self.channel_mock.basic_publish.call_args_list,
                         [call(msg=amqp_msg,
                               exchange='default_exchange',
                               routing_key='')])

    def test_add_trail_keys(self):
        datetime_mock = Mock()
        datetime_mock.now.return_value = datetime(2015, 6, 26, 11, 52)
        with patch('hived.trail.generate_step_id', return_value='step_id'),\
                patch('hived.trail.get_address', return_value='ip.address'),\
                patch('hived.process.get_name', return_value='name'), \
                patch(MODULE + 'datetime', datetime_mock):
            message = add_trail_keys({}, 'exchange', 'routing_key')

        self.assertEqual(message, {STEP_FIELD: {'exchange': 'exchange',
                                                'process': 'name',
                                                'routing_key': 'routing_key',
                                                'address': 'ip.address',
                                                'time': '2015-06-26T11:52:00'},
                                   TRAIL_FIELD: {'id_': 'trail_id', 'live': 'live', 'steps': ['step_id']}})

    def test_put_adds_trail_key_and_step_info_to_messages_sent(self):
        datetime_mock = Mock()
        datetime_mock.now.return_value = datetime(2015, 6, 26, 11, 52)
        with patch(MODULE + 'add_trail_keys', return_value={'key': 'value', 'trail': 'field'}),\
                patch(MODULE + 'Message') as MockMessage:
            self.external_queue.put(message_dict={'key': 'value'}, exchange='exchange', routing_key='routing_key')

        self.trail['steps'] = ['step_id']
        self.assertEqual(MockMessage.call_args_list,
                         [call(json.dumps({'key': 'value', 'trail': 'field'}),
                               delivery_mode=2, content_type='application/json', priority=0)])

    def test_put_serializes_message_if_necessary(self):
        message = {'key': 'value'}
        with patch(MODULE + 'Message') as MockMessage:
            self.external_queue.put(message_dict=message)

        self.assertEqual(MockMessage.call_args_list,
                         [call(json.dumps(message), delivery_mode=2,
                               content_type='application/json', priority=0)])

    def test_put_raises_serialization_error_if_message_cant_be_serialized_to_json(self):
        self.assertRaises(SerializationError, self.external_queue.put, message_dict=ValueError)

    def test_passes_priority_to_message_object(self):
        body = Mock()
        with patch(MODULE + 'Message') as MockMessage,\
                patch('hived.trail.get_priority', return_value=0):
            self.external_queue.put(body=body, priority=1)

        self.assertEqual(MockMessage.call_args_list,
                         [call(body, delivery_mode=2,
                               content_type='application/json', priority=1)])

    def test_put_uses_trail_priority(self):
        body = Mock()
        with patch(MODULE + 'Message') as MockMessage,\
                patch('hived.trail.get_priority', return_value=42):
            self.external_queue.put(body=body)

        self.assertEqual(MockMessage.call_args_list,
                         [call(body, delivery_mode=2,
                                    content_type='application/json', priority=42)])

    def test_get_uses_default_queue_if_not_supplied(self):
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list,
                         [call(queue='default_queue')])

    def test_get_returns_none_if_block_is_false_and_queue_is_empty(self):
        self.channel_mock.basic_get.return_value = None
        rv = self.external_queue.get(block=False)
        self.assertEqual(rv, (None, None))

    def test_get_sleeps_and_tries_again_until_queue_is_not_empty(self):
        empty_rv = None
        self.channel_mock.basic_get.side_effect = [empty_rv, empty_rv, self.message]
        with patch('time.sleep') as sleep,\
                patch(MODULE + 'ExternalQueue._parse_message') as parse_message_mock:
            message = self.external_queue.get(queue_name='queue_name')

            self.assertEqual(message, parse_message_mock.return_value)
            self.assertEqual(parse_message_mock.call_args_list,
                             [call(self.message)])
            self.assertEqual(self.channel_mock.basic_get.call_args_list,
                             [call(queue='queue_name'),
                              call(queue='queue_name'),
                              call(queue='queue_name')])
            self.assertEqual(sleep.call_count, 2)

    def test_get_crashes_if_default_queue_does_not_exist(self):
        self.connection_cls_mock.return_value.channel.side_effect = ConnectionError
        with self.assertRaises(ConnectionError):
            self.external_queue.get()

    def test_parse_message_deserializes_the_message_body_and_sets_meta_field(self):
        message, ack = self.external_queue._parse_message(self.message)
        self.assertEqual(message[META_FIELD], {})
        self.assertEqual(ack, 'delivery_tag')

    def test_parse_message_calls_set_trail(self):
        with patch('hived.trail.set_trail') as set_trail_mock:
            message, ack = self.external_queue._parse_message(self.message)
            self.assertEqual(set_trail_mock.call_args_list,
                             [call(id_='trail_id', live='live', steps=[])])
            self.assertEqual(ack, 'delivery_tag')

    def test_parse_message_traces_process_entered_event(self):
        with patch('hived.trail.trace') as trace_mock:
            self.external_queue._parse_message(self.message)
            self.assertEqual(trace_mock.call_args_list,
                             [call(type_=trail.EventType.process_entered)])

    def test_malformed_message_should_ack(self):
        queue = self.external_queue
        self.message.body = "{'foo': True}"
        with patch.object(queue, 'ack') as mock_ack:
            result = queue._parse_message(self.message)
            self.assertIsNone(result)
            self.assertEqual(mock_ack.call_args_list, [call(self.message.delivery_info['delivery_tag'])])

    def test_setup_consumer(self):
        callback = Mock()
        self.external_queue.connection = Mock()
        self.external_queue.channel = Mock()
        with patch(MODULE + 'ExternalQueue._try') as try_mock:
            self.external_queue.setup_consumer(callback, ['queue_1', 'queue_2'])

            self.assertEqual(try_mock.call_args_list,
                             [call('basic_qos', prefetch_size=0,
                                   prefetch_count=1, a_global=False)])
            self.assertEqual(self.external_queue.channel.basic_consume.call_args_list,
                             [call('queue_1', callback=ANY),
                              call('queue_2', callback=ANY)])

    def test_consume(self):
        self.external_queue.connection = Mock()
        self.external_queue.consume()
        self.assertEqual(self.external_queue.connection.drain_events.call_count, 1)

    def test_ack_ignores_connection_errors(self):
        self.external_queue.channel = self.channel_mock
        self.channel_mock.basic_ack.side_effect = AMQPError
        self.external_queue.ack('delivery_tag')

    def test_reject_ignores_connection_errors(self):
        self.external_queue.channel = self.channel_mock
        self.channel_mock.basic_reject.side_effect = AMQPError
        self.external_queue.reject('delivery_tag')

    def test_does_not_crash_on_context_management(self):
        queue = self.external_queue
        with queue as q:
            self.assertEqual(q, queue)
        # Do nothing to force close without connection

    def test_subscribe_declares_queue(self):
        with patch.object(self.external_queue, 'channel') as mock_channel:
            self.external_queue._subscribe()

            self.assertEqual(mock_channel.queue_declare.call_args_list, [call(queue=self.external_queue.default_queue_name, durable=False, exclusive=True, auto_delete=True)])

    def test_subscribe_binds_to_queue(self):
        with patch.object(self.external_queue, 'channel') as mock_channel:
            self.external_queue._subscribe()

            self.assertEqual(mock_channel.queue_bind.call_args_list, [call(exchange='notifications', queue=self.external_queue.default_queue_name, routing_key=self.external_queue.subscription)])

    def test_subscribe_sets_subscription(self):
        with patch.object(self.external_queue, '_connect'):
            self.external_queue.subscribe('routing_key')

            self.assertEqual('routing_key', self.external_queue.subscription)

    def test_subscribe_connects_to_server(self):
        with patch.object(self.external_queue, '_connect') as mock_connect:
            self.external_queue.subscribe('routing_key')

            self.assertEqual(mock_connect.call_count, 1)

    def test_message_callback_parses_message(self):
        def callback(message=None, delivery_tag=None):
            return message

        def stub_basic_consume(queue_name, callback):
            callback(self.message)

        mock_channel = Mock()
        mock_channel.basic_consume = stub_basic_consume

        with patch.object(self.external_queue, '_try'), \
             patch.object(self.external_queue, '_parse_message', return_value=self.message) as mock_parse_message:
            self.external_queue.channel = mock_channel
            self.external_queue.setup_consumer(callback, ['queue_1'])

            self.assertEqual(mock_parse_message.call_args_list, [call(self.message)])
