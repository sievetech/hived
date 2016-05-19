import unittest
from amqp import AMQPError

from mock import call, Mock, patch, ANY

from hived.worker import BaseWorker, BaseWorkerThread

CLASS = 'hived.worker.BaseWorker.'


class Worker(BaseWorker):

    def process_task(self, task):
        raise NotImplementedError


class BaseWorkerTest(unittest.TestCase):
    def setUp(self):
        self.worker = Worker(Mock(), 'queue_name')
        self.worker.queue = Mock()

    def test_base_worker_is_abstract(self):
        self.assertRaises(TypeError, BaseWorker, Mock(), 'queue_name')

    def test_init_adds_process_to_instance(self):
        process = Mock()
        worker = Worker(Mock(), process=process)
        self.assertEqual(worker.process, process)

    def test_warn_using_default_message_validation(self):
        worker = self.worker
        logger = worker.logger
        self.assertTrue(worker.validate_message({}))
        logger.warning.assert_called_once_with(
            '[%s %x] using fail-safe validate_message (always true)',
            worker, id(worker),
        )

    def test_send_invalid_messages_to_garbage(self):
        error = Mock()
        self.worker.send_message_to_garbage({'message': 'content'},
                                            'ack', error)

        self.assertEqual(self.worker.queue.put.call_args,
                         call({'garbage_reason': str(error),
                               'message': 'content'},
                              'queue_name_garbage'))
        self.assertEqual(self.worker.queue.ack.call_count, 1)

    def test_run_waits_before_restarting_after_a_crash(self):
        self.worker.consume_count = 0

        def consume_mock():
            if self.worker.consume_count:
                self.worker.stopped = True
            self.worker.consume_count += 1
            raise AssertionError

        self.worker.queue.consume = consume_mock
        setup_consumer = self.worker.queue.setup_consumer = Mock()
        with patch('time.sleep') as sleep_mock,\
                patch('random.randint', side_effect=[1, 2]):
            self.worker.run()

            self.assertEqual(self.worker.consume_count, 2)
            setup_consumer.assert_called_once_with(self.worker.on_message)
            self.assertEqual(sleep_mock.call_args_list, [call(1), call(3)])

    def test_run_calls_setup_consumer_again_after_amqp_error(self):
        def consume_mock():
            self.worker.stopped = True
            raise AMQPError

        self.worker.queue.consume = consume_mock
        setup_consumer = self.worker.queue.setup_consumer = Mock()
        with patch('time.sleep'):
            self.worker.run()

            self.assertEqual(setup_consumer.call_args_list,
                             [call(self.worker.on_message),
                              call(self.worker.on_message)])

    def test_run_calls_setup_consumer_again_after_io_error(self):
        def consume_mock():
            self.worker.stopped = True
            raise IOError

        self.worker.queue.consume = consume_mock
        setup_consumer = self.worker.queue.setup_consumer = Mock()
        with patch('time.sleep'):
            self.worker.run()

            self.assertEqual(setup_consumer.call_args_list,
                             [call(self.worker.on_message),
                              call(self.worker.on_message)])

    def test_run_logs_warning_and_debug_with_stack_trace_after_amqp_error(self):
        def consume_mock():
            self.worker.stopped = True
            raise AMQPError

        self.worker.queue.consume = consume_mock
        logger = self.worker.logger

        with patch('time.sleep'), patch('traceback.format_exc') as traceback:
            self.worker.run()

            self.assertEqual(logger.warning.call_count, 1)
            self.assertEqual(logger.debug.call_count, 1)
            self.assertEqual(traceback.call_count, 1)

    def test_run_logs_warning_and_debug_with_stack_trace_after_io_error(self):
        def consume_mock():
            self.worker.stopped = True
            raise IOError

        self.worker.queue.consume = consume_mock
        logger = self.worker.logger

        with patch('time.sleep'), patch('traceback.format_exc') as traceback:
            self.worker.run()

            self.assertEqual(logger.warning.call_count, 1)
            self.assertEqual(logger.debug.call_count, 1)
            self.assertEqual(traceback.call_count, 1)

    def test_run_logs_exception_with_stack_trace_after_unexpected_error(self):
        def consume_mock():
            self.worker.stopped = True
            raise Exception

        self.worker.queue.consume = consume_mock
        logger = self.worker.logger

        with patch('time.sleep'), patch('traceback.format_exc') as traceback:
            self.worker.run()

            self.assertEqual(logger.exception.call_count, 1)
            self.assertEqual(traceback.call_count, 1)

    def test_get_task_instantiates_task_class(self):
        class W(BaseWorker):
            task_class = Mock()

            def process_task(self, task):
                raise NotImplementedError

        worker = W(Mock(), 'myqueue')
        message = Mock()
        task = worker.get_task(message)
        self.assertEqual(task, W.task_class.return_value)
        W.task_class.assert_called_once_with(message)

    def test_get_task_always_return_message(self):
        message = Mock()
        task = self.worker.get_task(message)

        self.assertEqual(task, message)

    def test_sends_message_to_garbage_if_validate_message_fails(self):
        self.worker.validate_message = Mock(side_effect=AssertionError)
        message, delivery_tag = Mock(), Mock()

        with patch(CLASS + 'send_message_to_garbage') as send_to_garbage_mock:
            self.worker.on_message(message, delivery_tag)
            send_to_garbage_mock.assert_called_once_with(message, delivery_tag,
                                                         ANY)

    def test_sends_message_to_garbage_if_get_task_fails(self):
        self.worker.get_task = Mock(side_effect=AssertionError)
        message, delivery_tag = Mock(), Mock()

        with patch(CLASS + 'send_message_to_garbage') as send_to_garbage_mock:
            self.worker.on_message(message, delivery_tag)
            send_to_garbage_mock.assert_called_once_with(message, delivery_tag,
                                                         ANY)

    def test_on_message_calls_process_task(self):
        task = Mock()
        self.worker.get_task = Mock(return_value=task)
        message, delivery_tag = Mock(), Mock()

        with patch(CLASS + '_call_process_task') as call_process_task_mock:
            self.worker.on_message(message, delivery_tag)
            call_process_task_mock.assert_called_once_with(delivery_tag, task)

    def test_call_process_task_rejects_message_if_an_exception_is_raised(self):
        class MockException(Exception):
            pass

        delivery_tag, task = Mock(), Mock()
        worker = self.worker

        with patch.object(worker, 'process_task',
                          side_effect=MockException) as process_task_mock:
            self.assertRaises(MockException, worker._call_process_task,
                              delivery_tag, task)
            process_task_mock.assert_called_once_with(task)
            worker.queue.reject.assert_called_once_with(delivery_tag)

    def test_call_process_task_acks_message(self):
        delivery_tag, task = Mock(), Mock()
        worker = self.worker

        with patch.object(worker, 'process_task') as process_task_mock:
            worker._call_process_task(delivery_tag, task)

            process_task_mock.assert_called_once_with(task)
            worker.queue.ack.assert_called_once_with(delivery_tag)

    def test_process_task_raises_NotImplementedError(self):
        self.assertRaises(NotImplementedError, self.worker.process_task, Mock())


class TestBaseWorkerThread(unittest.TestCase):

    maxDiff = None

    def test_subclass_of_base_worker(self):
        self.assertTrue(issubclass(BaseWorkerThread, BaseWorker))
        base_worker_thread_dir = [attr for attr in dir(BaseWorkerThread)
                                  if attr != '__metaclass__']
        base_worker_dir = [attr for attr in dir(BaseWorker)
                           if attr != '__metaclass__']
        self.assertEqual(base_worker_thread_dir, base_worker_dir)

    @patch('hived.worker.warn')
    def test_deprecated(self, warn):
        assert not warn.called  # this is not the test, just a condition for it

        class Worker(BaseWorkerThread):
            def process_task(self, task):
                raise NotImplementedError

        warn.assert_called_once_with('use hived.worker.BaseWorker instead',
                                     DeprecationWarning)


if __name__ == '__main__':
    unittest.main()
