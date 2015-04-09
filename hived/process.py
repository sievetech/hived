import argparse
import logging
import logging.handlers
import os
import socket
import sys
from hived import conf

from hived.log import JsonFormatter


def get_logging_handlers(process_name, add_log_handler=False):
    """Returns a list of logging handlers, each with their level already set"""
    std_out = logging.StreamHandler(sys.stdout)
    std_out.setLevel(logging.DEBUG)
    handlers = [std_out]

    if conf.TEST:
        return handlers

    syslog = logging.handlers.SysLogHandler(address=('localhost', 5515), socktype=socket.SOCK_STREAM)
    syslog.setLevel(logging.INFO)
    handlers.append(syslog)

    if add_log_handler:
        path = os.path.expanduser('~/logs')
        if not os.path.isdir(path):
            os.mkdir(path)
        rotating_file = logging.handlers.RotatingFileHandler('%s/%s.log' % (path, process_name), maxBytes=10000000,
                                                             backupCount=10, encoding='utf-8')
        rotating_file.setLevel(logging.INFO)
        handlers.append(rotating_file)

    return handlers


def configure_logging(process_name, handlers):
    root = logging.getLogger('root')
    root.setLevel(logging.NOTSET)
    root.addHandler(logging.NullHandler())

    logger = logging.getLogger(process_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    formatter = JsonFormatter()
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


class Process(object):
    _created = False
    name = None
    worker_class = None
    default_workers = 1
    log_to_file = False  # Set to true for processes that have a low log rate and are important

    def __new__(cls, *args, **kwargs):
        if cls._created:
            raise RuntimeError('%s instance already created' % cls.__name__)

        cls._created = True
        return object.__new__(cls, *args, **kwargs)

    def get_arg_parser(self):
        parser = argparse.ArgumentParser(description='Start %s' % self.name)
        parser.add_argument('-w', '--workers', type=int, default=self.default_workers,
                            help='Number of worker threads to run')
        return parser

    def get_logging_handlers(self):
        return get_logging_handlers(self.name, add_log_handler=self.log_to_file)

    def configure_logging(self):
        return configure_logging(self.name, self.get_logging_handlers())

    def create_worker(self, args, logger):
        return self.worker_class(logger)

    def run(self):
        arg_parser = self.get_arg_parser()
        args = arg_parser.parse_args()
        logger = self.configure_logging()
        for i in range(args.workers):
            self.create_worker(args, logger).start()
