from threading import local
import uuid


_local = local()


def generate_id():
    return str(uuid.uuid4())


def get_id():
    id_ = getattr(_local, 'tracing_id', None)
    if not id_:
        id_ = generate_id()
        set_id(id_)
    return id_


def set_id(id_):
    _local.tracing_id = id_ or generate_id()
