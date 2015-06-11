import base64
from datetime import datetime
import os
import sys
from threading import local
import uuid

from raven.utils.stacks import iter_traceback_frames, get_stack_info

from hived import conf

_local = local()


def generate_id():
    return str(uuid.uuid4())


def generate_step_id():
    return base64.urlsafe_b64encode(os.urandom(6))


def get_id():
    return getattr(_local, 'id', None)


def is_live():
    return getattr(_local, 'live', False)


def get_steps():
    return getattr(_local, 'steps', [])


def get_trail():
    return {'id_': get_id(),
            'live': is_live(),
            'steps': get_steps()}


def set_trail(id_=None, live=False, steps=None):
    _local.id = id_ or generate_id()
    _local.live = live
    _local.steps = steps or []


def set_queue(queue):
    _local.queue = queue


class EventType:
    process_entered = 'entered'
    exception = 'exception'


def trace(type_=None, **event_data):
    if get_id() and not conf.TRACING_DISABLED and hasattr(_local, 'queue'):
        from hived import process  # ugh
        message = {'process': process.get_name(),
                   'type': type_,
                   'time': datetime.now().isoformat(),
                   'data': event_data}
        _local.queue.put(message, exchange='trail', routing_key='trace')


def trace_exception(e):
    try:
        exc_info = sys.exc_info()
        frames = iter_traceback_frames(exc_info[-1])
        trace(type_=EventType.exception, exc=str(e), stack=get_stack_info(frames))
    except Exception:
        pass
