import base64
import os
import sys
import uuid
from datetime import datetime
from threading import local
from urllib2 import urlopen

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


def get_priority():
    return getattr(_local, 'priority', 0)


def get_address():
    return getattr(_local, 'address', 'localhost')


def set_priority(priority):
    if not isinstance(priority, int):
        priority = int(bool(priority))
    _local.priority = priority


def get_steps():
    # Should return a new list, because new steps shouldn't be added to the task currently being processed
    return list(getattr(_local, 'steps', []))


def get_trail():
    trail = {'id_': get_id() or generate_id(),
             'live': is_live(),
             'priority': get_priority(),
             'steps': get_steps()}
    trail.update(getattr(_local, 'extra', {}))
    return trail


def set_trail(id_=None, live=False, priority=0, steps=None, **extra):
    _local.id = id_
    _local.live = live
    set_priority(priority)
    _local.steps = steps or []
    _local.extra = extra


def init_trail(queue):
    _local.queue = queue
    try:
        address = (urlopen(conf.EXTERNAL_IP_URL).headers['x-my-ip']
                   if conf.EXTERNAL_IP_URL
                   else 'localhost')
    except:
        address = 'localhost'
    _local.address = address


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
