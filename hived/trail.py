from datetime import datetime
import sys
from threading import local
import uuid

from raven.utils.stacks import iter_traceback_frames, get_stack_info

from hived import conf

_local = local()


def generate_id():
    return str(uuid.uuid4())


def get_id():
    return getattr(_local, 'id', None)


def is_live():
    return getattr(_local, 'live', False)


def get_trail():
    return {'id_': get_id(),
            'live': is_live()}


def set_trail(id_=None, live=False):
    _local.id = id_ or generate_id()
    _local.live = live


class EventType:
    process_entered = 'entered'
    exception = 'exception'


def trace(type_=None, **event_data):
    current_id = get_id()
    if current_id and conf.TRACING_ENABLED:
        from hived import process  # ugh
        if not hasattr(_local, 'queue'):
            from hived.queue import ExternalQueue
            _local.queue = ExternalQueue(conf.TRACING_QUEUE_HOST, conf.TRACING_QUEUE_USER, conf.TRACING_QUEUE_PASSWORD,
                                         exchange='trail')

        message = {'process': process.get_name(),
                   'type': type_,
                   'trail_id': current_id,
                   'live': is_live(),
                   'time': datetime.now().isoformat(),
                   'data': event_data}
        _local.queue.put(message, routing_key='trace')


def trace_exception(e):
    try:
        exc_info = sys.exc_info()
        frames = iter_traceback_frames(exc_info[-1])
        trace(type_=EventType.exception, exc=str(e), stack=get_stack_info(frames))
    except Exception:
        pass
