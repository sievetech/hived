import unittest

from mock import patch, call
from hived import tracing


class TracingTest(unittest.TestCase):
    def test_generates_id_using_uuid(self):
        with patch('uuid.uuid4', return_value='uuid'):
            self.assertEqual(tracing.generate_id(), 'uuid')

    def test_get_returns_existing_id(self):
        tracing._local.tracing_id = 42
        self.assertEqual(tracing.get_id(), 42)

    def test_get_generates_a_new_id_and_returns_it_if_there_is_no_current_id(self):
        tracing._local.tracing_id = None
        with patch('hived.tracing.generate_id', return_value=42),\
                patch('hived.tracing.set_id') as set_id_mock:
            self.assertEqual(tracing.get_id(), 42)
            self.assertEqual(set_id_mock.call_args_list, [call(42)])

    def test_set_id(self):
        tracing._local.tracing_id = None
        tracing.set_id(42)
        self.assertEqual(tracing._local.tracing_id, 42)

    def test_set_id_generates_a_new_id_if_given_a_null_one(self):
        tracing._local.tracing_id = 41
        with patch('hived.tracing.generate_id', return_value=42):
            tracing.set_id(None)
            self.assertEqual(tracing._local.tracing_id, 42)


if __name__ == '__main__':
    unittest.main()
