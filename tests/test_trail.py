from datetime import datetime
import unittest

from mock import patch, call, Mock
from hived import trail

MODULE_NAME = 'hived.trail'


class TrailTest(unittest.TestCase):
    def test_generates_id_using_uuid(self):
        with patch('uuid.uuid4', return_value='uuid'):
            self.assertEqual(trail.generate_id(), 'uuid')

    def test_get_returns_existing_id(self):
        trail._local.id = 42
        self.assertEqual(trail.get_id(), 42)

    def test_set_trail(self):
        trail._local.id = trail._local.live = None
        live = Mock()
        trail.set_trail(id_=42, live=live)
        self.assertEqual(trail._local.id, 42)
        self.assertEqual(trail._local.live, live)

    def test_set_trail_generates_a_new_id_if_given_a_null_one(self):
        trail._local.id = None
        trail._local.live = None
        with patch(MODULE_NAME + '.generate_id', return_value=42):
            trail.set_trail()
            self.assertEqual(trail._local.id, 42)
            self.assertFalse(trail._local.live)

    def test_trace_sends_message_to_queue(self):
        now = datetime(2015, 5, 4, 21, 10, 42)
        datetime_mock = Mock()
        datetime_mock.now.return_value = now
        type_ = Mock()
        with patch(MODULE_NAME + '._local.queue', create=True) as queue_mock,\
                patch(MODULE_NAME + '._local.id', 'trail_id'),\
                patch(MODULE_NAME + '._local.live', 'is_live'),\
                patch('hived.process.get_name', return_value='process_name'),\
                patch('hived.conf.TRACING_ENABLED', True),\
                patch(MODULE_NAME + '.datetime', datetime_mock):
            trail.trace(type_=type_, event='data')
            self.assertEqual(queue_mock.put.call_args_list,
                             [call({'time': '2015-05-04T21:10:42',
                                    'type': type_,
                                    'trail_id': 'trail_id',
                                    'live': 'is_live',
                                    'name': 'process_name',
                                    'data': {'event': 'data'}},
                                   routing_key='trace')])

    def test_trace_creates_external_queue_if_it_doesnt_exist_in_the_thread_local(self):
        if hasattr(trail._local, 'queue'):
            del trail._local.queue
        with patch(MODULE_NAME + '._local.id', 'trail_id'),\
                patch('hived.queue.ExternalQueue') as queue_mock,\
                patch('hived.conf.TRACING_QUEUE_HOST', 'host'),\
                patch('hived.conf.TRACING_QUEUE_USER', 'user'),\
                patch('hived.conf.TRACING_QUEUE_PASSWORD', 'password'),\
                patch('hived.conf.TRACING_ENABLED', True):
            trail.trace()
            self.assertEqual(queue_mock.call_args_list, [call('host', 'user', 'password', exchange='trail')])
            self.assertEqual(queue_mock.return_value.put.call_count, 1)

    def test_trace_doesnt_do_anything_if_tracing_is_not_enabled(self):
        if hasattr(trail._local, 'queue'):
            del trail._local.queue
        with patch(MODULE_NAME + '._local.id', 'trail_id'),\
                patch('hived.conf.TRACING_ENABLED', False),\
                patch('hived.queue.ExternalQueue') as queue_mock:
            trail.trace()
            self.assertEqual(queue_mock.call_count, 0)

    def test_trace_exception(self):
        exc = Mock()
        # Make .trace raise an exception to make sure trace_exception suppresses any generated exceptions
        with patch('sys.exc_info') as exc_info_mock,\
                patch(MODULE_NAME + '.iter_traceback_frames') as iter_frames_mock,\
                patch(MODULE_NAME + '.get_stack_info') as get_stack_mock,\
                patch(MODULE_NAME + '.trace', side_effect=Exception) as trace_mock:
            trail.trace_exception(exc)
            self.assertEqual(iter_frames_mock.call_args_list, [call(exc_info_mock.return_value[-1])])
            self.assertEqual(get_stack_mock.call_args_list, [call(iter_frames_mock.return_value)])
            self.assertEqual(trace_mock.call_args_list,
                             [call(type_=trail.EventType.exception, exc=str(exc), stack=get_stack_mock.return_value)])


if __name__ == '__main__':
    unittest.main()
