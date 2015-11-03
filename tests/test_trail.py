from datetime import datetime
import unittest

from mock import patch, call, Mock
from hived import trail

MODULE_NAME = 'hived.trail'


class TrailTest(unittest.TestCase):
    def test_generates_id_using_uuid(self):
        with patch('uuid.uuid4', return_value='uuid'):
            self.assertEqual(trail.generate_id(), 'uuid')

    def test_get_id_returns_existing_id(self):
        trail._local.id = 42
        self.assertEqual(trail.get_id(), 42)

    def test_get_trail(self):
        trail._local.id = Mock()
        trail._local.live = Mock()
        trail._local.priority = Mock()
        trail._local.steps = [Mock()]
        trail._local.extra = {'extra_1': 1, 'extra_2': 2}
        trail._local.address = Mock()
        returned_trail = trail.get_trail()
        self.assertEqual(returned_trail,
                         {'id_': trail._local.id,
                          'live': trail._local.live,
                          'priority': trail._local.priority,
                          'steps': trail._local.steps,
                          'extra_1': 1,
                          'extra_2': 2})
        self.assertIsNot(returned_trail['steps'], trail._local.steps)

    def test_get_trail_generates_a_new_id_if_the_current_is_none(self):
        trail._local.id = None
        with patch(MODULE_NAME + '.generate_id') as generate_mock:
            self.assertEqual(trail.get_trail()['id_'], generate_mock.return_value)

    def test_set_trail(self):
        trail._local.id = trail._local.live = None
        live = Mock()

        trail._local.priority = None
        priority = Mock()

        def set_priority(_):
            trail._local.priority = priority

        with patch(MODULE_NAME + '.set_priority', set_priority):
            trail.set_trail(id_=42, live=live, priority=Mock(), steps=[1, 2], extra_arg_1=1, extra_arg_2=2)

        self.assertEqual(trail._local.id, 42)
        self.assertEqual(trail._local.live, live)
        self.assertEqual(trail._local.priority, priority)
        self.assertEqual(trail._local.steps, [1, 2])
        self.assertEqual(trail._local.extra, {'extra_arg_1': 1, 'extra_arg_2': 2})

    def test_set_priority_converts_non_int_values(self):
        trail._local.priority = None

        trail.set_priority(False)
        self.assertEqual(trail.get_priority(), 0)

        trail.set_priority(None)
        self.assertEqual(trail.get_priority(), 0)

        trail.set_priority(True)
        self.assertEqual(trail.get_priority(), 1)

    def test_init_trail_without_address_url(self):
        trail._local.queue = None
        queue = Mock()
        trail.init_trail(queue)
        self.assertEqual(trail._local.queue, queue)
        self.assertEqual(trail._local.address, 'localhost')

    def test_init_trail_with_address_url(self):
        url = 'http://localhost/test_ip'
        ip = '192.168.24.106'
        with patch('hived.conf.EXTERNAL_IP_URL', url), \
             patch('hived.trail.urlopen', ) as url_open:
            # headers is not actually a dict IRL, but it implements __getitem__
            url_open().headers = {'x-my-ip': ip}
            trail.init_trail(Mock())
            self.assertEqual(trail._local.address, ip)

    def test_init_trail_sets_default_address_on_fail(self):
        url = 'http://localhost/test_ip'
        with patch('hived.conf.EXTERNAL_IP_URL', url), \
             patch('hived.trail.urlopen', side_effect=Exception):
            trail.init_trail(Mock())
            self.assertEqual(trail._local.address, 'localhost')

    def test_trace_sends_message_to_queue(self):
        now = datetime(2015, 5, 4, 21, 10, 42)
        datetime_mock = Mock()
        datetime_mock.now.return_value = now
        type_ = Mock()
        with patch(MODULE_NAME + '._local.queue', create=True) as queue_mock,\
                patch(MODULE_NAME + '._local.id', 'trail_id'),\
                patch(MODULE_NAME + '._local.live', 'is_live'),\
                patch('hived.process.get_name', return_value='process_name'),\
                patch('hived.conf.TRACING_DISABLED', False),\
                patch(MODULE_NAME + '.datetime', datetime_mock):
            trail.trace(type_=type_, event='data')
            self.assertEqual(queue_mock.put.call_args_list,
                             [call({'time': '2015-05-04T21:10:42',
                                    'type': type_,
                                    'process': 'process_name',
                                    'data': {'event': 'data'}},
                                   routing_key='trace',
                                   exchange='trail')])

    def test_trace_doesnt_do_anything_if_tracing_is_disabled(self):
        with patch(MODULE_NAME + '._local.id', 'trail_id'),\
                patch(MODULE_NAME + '._local.queue', create=True) as queue_mock,\
                patch('hived.conf.TRACING_DISABLED', True):
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
