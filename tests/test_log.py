import json
import unittest

import mock
from hived.log import json_handler, JsonFormatter


class SampleObject:
    def __repr__(self):
        return "I'm an object"


class ISOObject(SampleObject):
    def isoformat(self):
        return "ISOFORMAT"


class Record:
    def __init__(self, msg, event=None):
        self.msg = msg
        self.event = event
        self.args = {}
        self.name = 'name'
        self.levelname = 'info'
        self.created = 1455931391
        self.msecs = 200

        if event:
            self.args['event'] = event

    def getMessage(self):
        return self.msg


class JsonHandlerTest(unittest.TestCase):
    def test_json_handler(self):
        obj = SampleObject()

        self.assertEqual(json_handler(obj), "I'm an object")

    def test_json_handler_with_isoformat(self):
        obj = ISOObject()

        self.assertEqual(json_handler(obj), "ISOFORMAT")


class LogTest(unittest.TestCase):
    def setUp(self):
        self.formatter = JsonFormatter()

    def test_format_basic_record(self):
        record = Record('message')
        formatted = json.loads(self.formatter.format(record))

        self.assertEqual(formatted['msg'], 'message')
        self.assertEqual(formatted['_time'], '2016-02-19 23:23:11,200')
        self.assertEqual(formatted['_level'], record.levelname)
        self.assertEqual(formatted['_name'], record.name)

    def test_format_with_message_dict(self):
        record = Record({'msg': 'value'})
        formatted = json.loads(self.formatter.format(record))

        self.assertEqual(formatted['msg'], 'value')

    def test_format_with_record_event(self):
        record = Record('message', 'some event')
        formatted = json.loads(self.formatter.format(record))

        self.assertEqual(formatted['_ev'], 'some event')

    def test_format_with_trail_id(self):
        record = Record('message', 'some event')

        with mock.patch('hived.trail.get_id', return_value='trail_id'):
            formatted = json.loads(self.formatter.format(record))

            self.assertEqual(formatted['_trail_id'], 'trail_id')

