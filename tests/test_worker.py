import unittest

from mock import call, Mock, patch, ANY
from hived.queue import ConnectionError

from hived.worker import BaseWorker

CLASS = 'hived.worker.BaseWorker.'


class BaseWorkerTest(unittest.TestCase):
    def setUp(self):
        self.worker = BaseWorker(Mock(), 'queue_name')
        self.worker.queue = Mock()

    def test_init_adds_process_to_instance(self):
        process = Mock()
        worker = BaseWorker(Mock(), process=process)
        self.assertEqual(worker.process, process)

    def test_send_invalid_messages_to_garbage(self):
        error = Mock()
        self.worker.send_message_to_garbage({'message': 'content'},
                                            'ack', error)

        self.assertEqual(self.worker.queue.put.call_args,
                         call({'garbage_reason': str(error),
                               'message': 'content'},
                              'queue_name_garbage'))
        self.assertEqual(self.worker.queue.ack.call_count, 1)

    def test_worker_waits_before_restarting_after_a_crash(self):
        worker = BaseWorker(Mock(), 'myqueue')
        worker.already_called = False
        consume_args = []

        def consume_mock(*args):
            consume_args.append(args)
            if worker.already_called:
                worker.stopped = True
            else:
                worker.already_called = True
            raise AssertionError('bad bad bad')

        worker.queue = Mock(consume=consume_mock)
        with patch('time.sleep') as sleep_mock,\
                patch('random.randint', side_effect=[1, 2]):
            worker.run()
            self.assertEqual(consume_args,
                             [(worker.on_message,), (worker.on_message,)])
            self.assertEqual(sleep_mock.call_args_list, [call(1), call(3)])

    def test_get_task_instantiates_task_class(self):
        class W(BaseWorker):
            task_class = Mock()

        worker = W(Mock(), 'myqueue')
        message = Mock()
        task = worker.get_task(message)
        self.assertEqual(task, W.task_class.return_value)
        self.assertEqual(W.task_class.call_args_list, [call(message)])

    def test_sends_message_to_garbage_if_validate_message_fails(self):
        self.worker.validate_message = Mock(side_effect=AssertionError)
        message, delivery_tag = Mock(), Mock()

        with patch(CLASS + 'send_message_to_garbage') as send_to_garbage_mock:
            self.worker.on_message(message, delivery_tag)
            self.assertEqual(send_to_garbage_mock.call_args_list,
                             [call(message, delivery_tag, ANY)])

    def test_sends_message_to_garbage_if_get_task_fails(self):
        self.worker.get_task = Mock(side_effect=AssertionError)
        message, delivery_tag = Mock(), Mock()

        with patch(CLASS + 'send_message_to_garbage') as send_to_garbage_mock:
            self.worker.on_message(message, delivery_tag)
            self.assertEqual(send_to_garbage_mock.call_args_list,
                             [call(message, delivery_tag, ANY)])

    def test_on_message_calls_process_task(self):
        task = Mock()
        self.worker.get_task = Mock(return_value=task)
        message, delivery_tag = Mock(), Mock()

        with patch(CLASS + '_call_process_task') as call_process_task_mock:
            self.worker.on_message(message, delivery_tag)
            self.assertEqual(call_process_task_mock.call_args_list,
                             [call(delivery_tag, task)])

    def test_call_process_task_rejects_message_if_an_exception_is_raised(self):
        class MockException(Exception):
            pass

        delivery_tag, task = Mock(), Mock()
        with patch(CLASS + 'process_task',
                   side_effect=MockException) as process_task_mock:
            self.assertRaises(MockException, self.worker._call_process_task,
                              delivery_tag, task)
            self.assertEqual(process_task_mock.call_args_list, [call(task)])
            self.assertEqual(self.worker.queue.reject.call_args_list,
                             [call(delivery_tag)])

    def test_call_process_task_acks_message(self):
        delivery_tag, task = Mock(), Mock()
        with patch(CLASS + 'process_task') as process_task_mock:
            self.worker._call_process_task(delivery_tag, task)

            self.assertEqual(process_task_mock.call_args_list, [call(task)])
            self.assertEqual(self.worker.queue.ack.call_args_list,
                             [call(delivery_tag)])


if __name__ == '__main__':
    unittest.main()
