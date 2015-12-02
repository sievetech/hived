import unittest

from mock import call, Mock, patch, ANY

from hived.worker import BaseWorker


class BaseWorkerTest(unittest.TestCase):
    def test_init_adds_process_to_instance(self):
        process = Mock()
        worker = BaseWorker(Mock(), process=process)
        self.assertEqual(worker.process, process)

    def test_send_invalid_messages_to_garbage(self):
        queue_name = 'myqueue'
        worker = BaseWorker(Mock(), queue_name=queue_name)
        worker.queue = Mock()

        error = Mock()
        worker.send_message_to_garbage({'message': 'content'}, 'ack', error)

        self.assertEqual(worker.queue.put.call_args,
                         call({'garbage_reason': str(error), 'message': 'content'}, queue_name + '_garbage'))
        self.assertEqual(worker.queue.ack.call_count, 1)

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

    def test_protected_run_sends_message_to_garbage_if_validate_message_fails(self):
        class W(BaseWorker):
            def validate_message(self, message):
                self.stopped = True
                assert False

        worker = W(Mock(), 'myqueue')
        worker.queue = Mock()
        message, ack = Mock(), Mock()

        with patch.object(worker, 'send_message_to_garbage') as send_to_garbage_mock:
            worker.on_message(message, ack)
            self.assertEqual(send_to_garbage_mock.call_args_list, [call(message, ack, ANY)])

    def test_protected_run_sends_message_to_garbage_if_get_task_fails(self):
        class W(BaseWorker):
            def get_task(self, message):
                self.stopped = True
                assert False

        worker = W(Mock(), 'myqueue')
        worker.queue = Mock()
        message, ack = Mock(), Mock()

        with patch.object(worker, 'send_message_to_garbage') as send_to_garbage_mock:
            worker.on_message(message, ack)
            self.assertEqual(send_to_garbage_mock.call_args_list, [call(message, ack, ANY)])


if __name__ == '__main__':
    unittest.main()
