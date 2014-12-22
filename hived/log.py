import logging

from hived import tracing
import simplejson as json


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
        if tracing.get_id():
            record_dict['_tracing_id'] = tracing.get_id()

        return json.dumps(record_dict, default=json_handler)
