import logging

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

        return json.dumps(record_dict, default=json_handler)
