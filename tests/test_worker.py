import time
import unittest

from mock import call, Mock, patch

from hived.worker import BaseWorker


class BaseWorkerTest(unittest.TestCase):
    def test_bad_messages_are_thrown_out(self):
        exc = AssertionError('bad bad bad')

        class W(BaseWorker):
            def validate_message(self, body):
                # make sure it halts
                self.stopped = True
                raise exc

        queue_name = 'myqueue'
        w = W(Mock(), queue_name)
        w.queue = Mock()
        w.queue.get.return_value = ({}, 1)

        w.run()

        self.assertEqual(w.queue.put.call_args,
                         call({'garbage_reason': str(exc)},
                              queue_name + '_garbage',
                              exchange=''))
        self.assertTrue(w.queue.ack.called)

    def test_worker_waits_before_restarting_after_a_crash(self):
        exc = AssertionError('bad bad bad')

        class W(BaseWorker):
            already_called = False

            def protected_run(self):
                # make sure it halts 
                if self.already_called:
                    self.stopped = True
                else:
                    self.already_called = True
                raise exc

        w = W(Mock(), 'myqueue')
        with patch.object(time, 'sleep') as sleep:
            w.run()
            sleep_times = [c[0][0] for c in sleep.call_args_list]
            self.assertLess(sleep_times[0], sleep_times[1])


if __name__ == '__main__':
    unittest.main()
