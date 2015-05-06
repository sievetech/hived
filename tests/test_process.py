import logging
import unittest

from mock import Mock, patch, call
import sys

from hived.process import Process


class SampleProcess(Process):
    name = 'process_name'
    worker_class = Mock()
    default_workers = 42
    default_queue_name = 'default_queue'


class ProcessTest(unittest.TestCase):
    def setUp(self):
        SampleProcess._created = False
        SampleProcess.worker_class = Mock()
        self.process = SampleProcess()

    def test_raises_runtime_error_if_we_try_to_create_two_instances(self):
        self.assertRaises(RuntimeError, SampleProcess)

    def test_creates_arg_parser(self):
        with patch('argparse.ArgumentParser') as parser_cls:
            parser = self.process.get_arg_parser()

            self.assertEqual(parser, parser_cls.return_value)
            self.assertEqual(parser_cls.call_args_list, [call(description='Start process_name')])
            self.assertEqual(parser.add_argument.call_args_list,
                             [call('-w', '--workers', default=42, type=int, help='Number of worker threads to run'),
                              call('-q', '--queue', default='default_queue', help='queue name')])

    def test_creates_logging_handlers(self):
        with patch('logging.StreamHandler') as stream_handler,\
                patch('logging.handlers.RotatingFileHandler') as rotating_file_handler,\
                patch('os.path.expanduser', return_value='home_dir'),\
                patch('os.path.isdir', return_value=True):
            handlers = self.process.get_logging_handlers()

            self.assertEqual(handlers, [stream_handler.return_value, rotating_file_handler.return_value])

            self.assertEqual(stream_handler.call_args_list, [call(sys.stdout)])
            self.assertEqual(stream_handler.return_value.setLevel.call_args_list, [call(logging.DEBUG)])

            self.assertEqual(rotating_file_handler.call_args_list,
                             [call('home_dir/process_name.log', maxBytes=10000000, backupCount=10, encoding='utf-8')])
            self.assertEqual(rotating_file_handler.return_value.setLevel.call_args_list, [call(logging.INFO)])

    def test_create_logging_handlers_creates_log_dir_if_it_doesnt_exist(self):
        with patch('os.path.expanduser', return_value='home_dir'),\
                patch('os.path.isdir', return_value=False) as isdir_mock,\
                patch('logging.handlers.RotatingFileHandler'),\
                patch('os.mkdir') as mkdir_mock:
            self.process.get_logging_handlers()

            isdir_mock.return_value = True
            self.process.get_logging_handlers()

            self.assertEqual(mkdir_mock.call_args_list, [call('home_dir')])

    def test_configures_logging(self):
        root_logger, process_logger = Mock(), Mock()
        handlers = [Mock(), Mock()]
        with patch('logging.getLogger', side_effect=[root_logger, process_logger]) as get_logger_mock,\
                patch('logging.NullHandler') as null_handler_mock,\
                patch('hived.process.JsonFormatter') as formatter_mock,\
                patch.object(self.process, 'get_logging_handlers', return_value=handlers):
            logger = self.process.configure_logging()

            self.assertEqual(logger, process_logger)
            self.assertEqual(get_logger_mock.call_args_list, [call('root'), call(self.process.name)])

            self.assertEqual(root_logger.setLevel.call_args_list, [call(logging.NOTSET)])
            self.assertEqual(root_logger.addHandler.call_args_list, [call(null_handler_mock.return_value)])

            self.assertEqual(process_logger.setLevel.call_args_list, [call(logging.DEBUG)])
            self.assertFalse(process_logger.propagate)

            self.assertEqual(formatter_mock.call_count, 1)
            self.assertEqual(handlers[0].setFormatter.call_args_list, [call(formatter_mock.return_value)])
            self.assertEqual(handlers[1].setFormatter.call_args_list, [call(formatter_mock.return_value)])
            self.assertEqual(process_logger.addHandler.call_args_list, [call(handlers[0]), call(handlers[1])])

    def test_creates_worker(self):
        logger = Mock()
        args = Mock()
        worker = self.process.create_worker(args, logger)
        self.assertEqual(worker, SampleProcess.worker_class.return_value)
        self.assertEqual(SampleProcess.worker_class.call_args_list, [call(logger, queue_name=args.queue)])

    def test_run(self):
        arg_parser, args = Mock(), Mock()
        arg_parser.parse_args.return_value = args
        args.workers = 2
        logger = Mock()
        workers = [Mock(), Mock()]
        with patch.object(self.process, 'get_arg_parser', return_value=arg_parser),\
                patch.object(self.process, 'configure_logging', return_value=logger),\
                patch.object(self.process, 'create_worker', side_effect=workers) as create_worker_mock:
            self.process.run()

            self.assertEqual(create_worker_mock.call_args_list, [call(args, logger), call(args, logger)])
            self.assertEqual(workers[0].start.call_count, 1)
            self.assertEqual(workers[1].start.call_count, 1)


if __name__ == '__main__':
    unittest.main()
