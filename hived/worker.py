import random
from threading import Thread
import traceback

import time
from hived import conf
from hived import trail
from hived.queue import ExternalQueue, SerializationError


class BaseWorker(Thread):
    """
    Base worker class.

    It should be extended by implementing the methods
    process_task() and, optionally, validate_message().
    It does not begin running until the run() method is called.
    """
    publisher_exchange = None
    task_class = None

    def __init__(self, logger, queue_name=None, queue_host=conf.QUEUE_HOST, queue_username=conf.QUEUE_USER,
                 queue_password=conf.QUEUE_PASSWORD, queue_virtual_host='/', process=None, queue_heartbeat=None):
        count = getattr(self.__class__, '__instance_count', 0)
        count += 1
        setattr(self.__class__, '__instance_count', count)
        Thread.__init__(self, name='%s-%s' % (self.__class__.__name__, count))

        self.logger = logger
        self.queue_name = queue_name
        if isinstance(queue_name, str):
            self.garbage_queue_name = queue_name + '_garbage'
        else:
            self.garbage_queue_name = None
        self.queue = ExternalQueue(
            host=queue_host,
            exchange=self.publisher_exchange,
            queue_name=queue_name,
            virtual_host=queue_virtual_host,
            username=queue_username,
            password=queue_password,
            queue_heartbeat=queue_heartbeat
        )
        self.process = process
        self.stopped = False

    def run(self):
        self.logger.info('%s started' % self)
        trail.init_trail(self.queue)  # Needs to be run inside the thread

        wait_time = 0
        while not self.stopped:
            wait_time += random.randint(1, 10)
            if wait_time > 60:
                wait_time = 0

            try:
                self.protected_run()
            except Exception as e:
                self.logger.exception({'exception': traceback.format_exc()})
                trail.trace_exception(e)
                time.sleep(wait_time)

    def send_message_to_garbage(self, message, delivery_tag, error):
        self.logger.info('Sending message to garbage queue: %s. Error: %s', message, error)
        message['garbage_reason'] = str(error)
        self.queue.put(message, self.garbage_queue_name)
        self.queue.ack(delivery_tag)

    def protected_run(self):
        while not self.stopped:
            try:
                message, delivery_tag = self.queue.get()
            except SerializationError as e:
                self.logger.info('Error deserializing message: %s', e.body)
                continue

            try:
                assert self.validate_message(message)
                task = self.get_task(message)
            except (AssertionError, TypeError, KeyError, ValueError) as e:
                self.send_message_to_garbage(message, delivery_tag, e)
            else:
                try:
                    self.process_task(task)
                except:
                    self.queue.reject(delivery_tag)
                    raise
                else:
                    self.queue.ack(delivery_tag)

    def validate_message(self, message):
        """
        Validates whether a message should be processed.
        message: the deserialized json

        A message is considered valid when this method returns True.
        If it returns False or raises an exception, the message is ignored
        and requeued to a garbage queue.
        """
        return True

    def get_task(self, message):
        if self.task_class is not None:
            return self.task_class(message)
        return message

    def process_task(self, task):
        """
        Does the actual processing of the task (should be implemented
        on derived classes).
        task: a deserialized json (the message).
        """
        raise NotImplementedError()

    def __repr__(self):
        return self.name


# Deprecated
BaseWorkerThread = BaseWorker


class SubscriberWorkerThread(BaseWorker):
    subscription_routing_key = None

    def __init__(self, queue_virtual_host='notifications', *args, **kwargs):
        super(SubscriberWorkerThread, self).__init__(queue_virtual_host=queue_virtual_host, *args, **kwargs)
        self.queue.subscribe(self.subscription_routing_key)
