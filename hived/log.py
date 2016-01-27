import logging
from traceback import format_exception
import simplejson as json

from hived import trail


def json_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        return repr(obj)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        if isinstance(record.msg, dict):
            record_dict = record.msg
        else:
            record_dict = {'msg': record.getMessage()}

        if 'event' in record.args:
            record_dict['_ev'] = record.args['event']
        record_dict['_time'] = self.formatTime(record)
        record_dict['_level'] = record.levelname
        record_dict['_name'] = record.name
        if trail.get_id():
            record_dict['_trail_id'] = trail.get_id()

        if record.exc_info:
            etype, exc, tb = record.exc_info
            record_dict['error'] = {
                'etype': etype.__name__,
                'exc': exc,
                'locals': {k: repr(v)
                           for k, v in tb.tb_frame.f_locals.iteritems()
                           if not k.startswith('__')},
                'traceback': format_exception(etype, exc, tb),
            }

        return json.dumps(record_dict, default=json_handler)
