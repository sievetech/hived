import unittest

from mock import patch, Mock

from hived.conf import get_var


class ConfTest(unittest.TestCase):
    def setUp(self):
        self.get_env_patcher = patch('os.getenv', return_value='env_var')
        self.get_env_mock = self.get_env_patcher.start()

        self.project_conf = Mock()
        self.project_conf.var = 'project_var'
        self.project_env_patcher = patch('hived.conf.project_conf', self.project_conf)
        self.project_env_patcher.start()

    def tearDown(self):
        self.get_env_patcher.stop()
        self.project_env_patcher.stop()

    def test_get_var_returns_env_var_if_set(self):
        self.assertEqual(get_var('var', 'default'), 'env_var')

    def test_get_var_returns_project_var_if_set(self):
        self.get_env_mock.return_value = None
        self.assertEqual(get_var('var', 'default'), 'project_var')

    def test_get_var_returns_default_if_var_is_not_set(self):
        self.get_env_mock.return_value = None
        self.project_conf.var = None
        self.assertEqual(get_var('var', 'default'), 'default')
