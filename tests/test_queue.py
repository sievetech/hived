import json
import unittest

from amqp import AMQPError, ConnectionError
import amqp
import mock

from hived.queue import ExternalQueue, MAX_TRIES, SerializationError, META_FIELD, TRACING_ID_FIELD


class ExternalQueueTest(unittest.TestCase):
    def setUp(self):
        _delivery_info = {'delivery_tag': 'delivery_tag'}

        self.message = mock.MagicMock()
        self.message.body = json.dumps({TRACING_ID_FIELD: 42})
        self.message.delivery_info = _delivery_info

        self.channel_mock = mock.MagicMock()
        self.channel_mock.basic_get.return_value = self.message

        self.connection = mock.MagicMock()
        self.connection.channel.return_value = self.channel_mock

        self.connection_cls_patcher = mock.patch('amqp.Connection',
                                                 return_value=self.connection)
        self.connection_cls_mock = self.connection_cls_patcher.start()

        self.tracing_id = 'tracing_id'
        self.tracing_patcher = mock.patch('hived.tracing.get_id', return_value=self.tracing_id)
        self.tracing_patcher.start()

        self.external_queue = ExternalQueue('localhost', 'username', 'pwd',
                                            exchange='default_exchange',
                                            queue_name='default_queue')

    def tearDown(self):
        self.connection_cls_patcher.stop()
        self.tracing_patcher.stop()

    def test__try_connects_if_disconnected(self):
        self.channel_mock.method.return_value = 'rv'
        rv = self.external_queue._try('method', arg='value')

        self.assertEqual(self.connection_cls_mock.call_count, 1)
        self.assertEqual(self.channel_mock.method.call_args_list,
                         [mock.call(arg='value')])
        self.assertEqual(rv, 'rv')

    def test__try_tries_up_to_max_tries(self):
        self.channel_mock.method.side_effect = [AMQPError, AMQPError, 'rv']
        rv = self.external_queue._try('method')

        self.assertEqual(self.channel_mock.method.call_count, MAX_TRIES)
        self.assertEqual(rv, 'rv')

    def test__try_doesnt_try_more_than_max_tries(self):
        self.channel_mock.method.side_effect = [AMQPError, AMQPError, AMQPError, 'rv']
        self.assertRaises(ConnectionError, self.external_queue._try, 'method')

    def test_put_uses_default_exchange_if_not_supplied(self):
        amqp_msg = amqp.basic_message.Message("body",
                                              delivery_mode=2,
                                              content_type='application/json')

        self.external_queue.put(body='body')
        self.assertEqual(self.channel_mock.basic_publish.call_args_list,
                         [mock.call(msg=amqp_msg,
                                    exchange='default_exchange',
                                    routing_key='')])

    def test_put_adds_tracing_id_to_messages(self):
        message_dict = {'key': 'value', TRACING_ID_FIELD: self.tracing_id}
        amqp_msg = amqp.basic_message.Message(json.dumps(message_dict),
                                              delivery_mode=2,
                                              content_type='application/json')

        self.external_queue.put(message_dict={'key': 'value'})
        self.assertEqual(self.channel_mock.basic_publish.call_args_list[0][1]['msg'],
                         amqp_msg)

    def test_put_serializes_message_if_necessary(self):
        message_dict = {'key': 'value', TRACING_ID_FIELD: None}
        amqp_msg = amqp.basic_message.Message(json.dumps(message_dict),
                                              delivery_mode=2,
                                              content_type='application/json')

        self.external_queue.put(message_dict=message_dict,
                                exchange='exchange',
                                routing_key='routing_key')
        self.assertEqual(self.channel_mock.basic_publish.call_args_list,
                         [mock.call(msg=amqp_msg,
                                    exchange='exchange',
                                    routing_key='routing_key')])

    def test_put_raises_serialization_error_if_message_cant_be_serialized_to_json(self):
        self.assertRaises(SerializationError, self.external_queue.put, message_dict=ValueError)

    def test_get_uses_default_queue_if_not_supplied(self):
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue')])

    def test_get_returns_none_if_block_is_false_and_queue_is_empty(self):
        self.channel_mock.basic_get.return_value = None
        rv = self.external_queue.get(block=False)
        self.assertEqual(rv, (None, None))

    def test_get_deserializes_the_message_body_and_sets_meta_field(self):
        message, ack = self.external_queue.get()
        self.assertEqual(message[META_FIELD], {})
        self.assertEqual(ack, 'delivery_tag')

    def test_get_sets_the_tracing_id(self):
        with mock.patch('hived.tracing.set_id') as set_id_mock:
            message, ack = self.external_queue.get()
            self.assertEqual(set_id_mock.call_args_list, [mock.call(42)])
            self.assertEqual(ack, 'delivery_tag')

    def test_get_raises_serialization_error_if_message_body_cant_be_parsed(self):
        self.message.body = ValueError
        self.assertRaises(SerializationError, self.external_queue.get)

    def test_get_sleeps_and_tries_again_until_queue_is_not_empty(self):
        empty_rv = None
        self.channel_mock.basic_get.side_effect = [empty_rv, empty_rv, self.message]
        with mock.patch('time.sleep') as sleep:
            _, delivery_tag = self.external_queue.get(queue_name='queue_name')

            self.assertEqual(self.channel_mock.basic_get.call_args_list,
                             [mock.call(queue='queue_name'),
                              mock.call(queue='queue_name'),
                              mock.call(queue='queue_name')])
            self.assertEqual(sleep.call_count, 2)
            self.assertEqual(delivery_tag, 'delivery_tag')

    def test_get_crashes_if_default_queue_does_not_exist(self):
        self.connection_cls_mock.return_value.channel.side_effect = ConnectionError
        with self.assertRaises(ConnectionError):
            self.external_queue.get()

    def test_get_does_not_crash_if_priority_queue_does_not_exist(self):
        is_priority = [True]

        def side_effect():
            # The first call is from priority queue
            if is_priority[0]:
                is_priority[0] = False
                raise ConnectionError
            return self.channel_mock
        self.connection_cls_mock.return_value.channel.side_effect = side_effect
        external_queue = ExternalQueue('localhost', 'username', 'pwd',
                                       exchange='default_exchange',
                                       queue_name='default_queue',
                                       priority=True)
        with mock.patch('hived.queue.warnings') as warnings:
            self.assertEqual(external_queue.get(), ({META_FIELD: {}, TRACING_ID_FIELD: 42}, 'delivery_tag'))
            # TODO: use logging instead of warnings
            warnings.warn.assert_called_once_with('priority queue does not exist: default_queue_priority')

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


class PriorityQueueTest(unittest.TestCase):
    def setUp(self):
        _delivery_info = {'delivery_tag': 'delivery_tag'}

        self.message = mock.MagicMock()
        self.message.body = "{}"
        self.message.delivery_info = _delivery_info

        self.channel_mock = mock.MagicMock()
        self.channel_mock.basic_get.return_value = self.message

        self.connection = mock.MagicMock()
        self.connection.channel.return_value = self.channel_mock

        self.connection_cls_patcher = mock.patch('amqp.Connection',
                                                 return_value=self.connection)
        self.connection_cls_mock = self.connection_cls_patcher.start()

        self.external_queue = ExternalQueue('localhost', 'username', 'pwd',
                                            exchange='default_exchange',
                                            queue_name='default_queue',
                                            priority=True)

    def tearDown(self):
        self.connection_cls_patcher.stop()

    def test_try_another_queue_on_failure(self):
        self.channel_mock.basic_get.return_value = None
        self.external_queue.get(block=False)
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue_priority'),
                                                                      mock.call(queue='default_queue')])

    def test_get_uses_priority_queue_0(self):
        self.external_queue.priority_count = 0
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue_priority')])
        self.assertEqual(self.external_queue.priority_count, 1)

    def test_get_uses_priority_queue_1(self):
        self.external_queue.priority_count = 1
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue_priority')])
        self.assertEqual(self.external_queue.priority_count, 2)

    def test_get_uses_default_queue_2(self):
        self.external_queue.priority_count = 2
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue')])
        self.assertEqual(self.external_queue.priority_count, 3)

    def test_get_uses_priority_queue_3(self):
        self.external_queue.priority_count = 3
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue_priority')])
        self.assertEqual(self.external_queue.priority_count, 4)

    def test_get_uses_priority_queue_4(self):
        self.external_queue.priority_count = 4
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue_priority')])
        self.assertEqual(self.external_queue.priority_count, 5)

    def test_get_uses_default_queue_5(self):
        self.external_queue.priority_count = 5
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue')])
        self.assertEqual(self.external_queue.priority_count, 6)

    def test_get_uses_priority_queue_6(self):
        self.external_queue.priority_count = 6
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue_priority')])
        self.assertEqual(self.external_queue.priority_count, 7)

    def test_get_uses_priority_queue_7(self):
        self.external_queue.priority_count = 7
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue_priority')])
        self.assertEqual(self.external_queue.priority_count, 8)

    def test_get_uses_default_queue_8(self):
        self.external_queue.priority_count = 8
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue')])
        self.assertEqual(self.external_queue.priority_count, 9)

    def test_get_uses_priority_queue_9(self):
        self.external_queue.priority_count = 9
        self.external_queue.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list, [mock.call(queue='default_queue_priority')])
        self.assertEqual(self.external_queue.priority_count, 0)


if __name__ == '__main__':
    unittest.main()
