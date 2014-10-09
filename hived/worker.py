import random
import time
from threading import Thread

from .queue import ExternalQueue, SerializationError


class BaseWorker(object):
    """
    Base worker class.

    It should be extended by implementing the methods
    process_task() and, optionally, validate_message().
    It does not begin running until the run() method is called.
    """
    publisher_exchange = None

    def __init__(self, logger,
                 queue_name=None,
                 queue_host='localhost',
                 queue_username='guest',
                 queue_password='guest',
                 queue_virtual_host='/'):
        self.logger = logger
        self.queue_name = queue_name
        if isinstance(queue_name, str):
            self.garbage_queue_name = queue_name + '_garbage'
        else:
            self.garbage_queue_name = None
        self.queue = ExternalQueue(host=queue_host,
                                   exchange=self.publisher_exchange,
                                   queue_name=queue_name,
                                   virtual_host=queue_virtual_host,
                                   username=queue_username,
                                   password=queue_password)
        self.stopped = False

    def run(self):
        self.logger.info('%s started' % self)
        wait_time = 0
        while not self.stopped:
            wait_time += random.randint(1, 10)
            if wait_time > 60:
                wait_time = 0

            try:
                self.protected_run()
            except Exception as e:
                m = '%s died, restarting in %s seconds. Exception: %s'
                self.logger.exception(m, self, wait_time, e)
                time.sleep(wait_time)

    def protected_run(self):
        while not self.stopped:
            try:
                message, delivery_tag = self.queue.get()
            except SerializationError as e:
                self.logger.info('Error deserializing message: %s', e.body)
                continue

            try:
                assert self.validate_message(message)
            except (AssertionError, KeyError, ValueError) as e:
                m = 'Sending message to garbage queue: %s. Error: %s'
                self.logger.info(m, message, e)
                message['garbage_reason'] = str(e)
                self.queue.put(message, self.garbage_queue_name, exchange='')
                self.queue.ack(delivery_tag)
            else:
                try:
                    self.process_message(message, delivery_tag)
                except:
                    self.queue.reject(delivery_tag)
                    raise

    def validate_message(self, body):
        """
        Validates wether a message should be processed.
        body: a deserialized json (the message)

        A message is considered valid when this method returns True.
        If it returns False or raises and exception, the message is ignored
        and requeued to a garbage queue.
        """
        return True

    def process_message(self, message, delivery_tag):
        self.process_task(message)
        self.queue.ack(delivery_tag)

    def process_task(self, task):
        """
        Does the actual processing of the task (should be implemented
        on derived classes).
        task: a deserialized json (the message).
        """
        raise NotImplementedError()


class BaseWorkerThread(BaseWorker, Thread):
    def __init__(self, *args, **kwargs):
        BaseWorker.__init__(self, *args, **kwargs)

        count = getattr(self.__class__, '__instance_count', 0)
        count += 1
        setattr(self.__class__, '__instance_count', count)
        Thread.__init__(self, name='%s-%s' % (self.__class__.__name__, count))

    def __repr__(self):
        return self.name


class SubscriberWorkerThread(BaseWorkerThread):
    subscription_routing_key = None

    def __init__(self, logger):
        super(SubscriberWorkerThread, self).__init__(logger, queue_virtual_host='notifications')
        self.queue.subscribe(self.subscription_routing_key)

