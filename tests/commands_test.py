from unittest import TestCase
from unittest.mock import Mock, patch

from nyuki.commands import get_command_kwargs


class TestCommand(TestCase):

    @patch('nyuki.commands._build_args')
    def test_001_get_command_kwargs(self, _build_args):
        # Arguments parsed
        args = Mock()
        args.config = 'config.json'
        args.server = '127.0.0.1:5555'
        args.api = 'localhost:8082'
        args.logging = 'DEBUG'
        _build_args.return_value = args

        # Result
        kwargs = get_command_kwargs()
        expected = {
            'bus': {
                'host': '127.0.0.1',
                'port': 5555,
            },
            'api': {
                'host': 'localhost',
                'port': 8082
            },
            'config': 'config.json',
            'log': {
                'root': {
                    'level': 'DEBUG'
                }
            }
        }
        self.assertEqual(kwargs, expected)
