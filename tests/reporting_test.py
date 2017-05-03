from datetime import datetime
from jsonschema import ValidationError
from nose.tools import assert_raises, eq_, assert_true
from asynctest import TestCase, CoroutineMock, ignore_loop, exhaust_callbacks

from nyuki.utils import utcnow
from nyuki.bus import reporting


class ReportingTest(TestCase):

    def setUp(self):
        self.publisher = CoroutineMock()
        self.publisher.SERVICE = 'xmpp'
        reporting.init('test', self.publisher)

    async def tearDown(self):
        await exhaust_callbacks(self.loop)

    @ignore_loop
    def test_001_check_schema(self):
        with assert_raises(ValidationError):
            reporting.check_report({
                'type': 'something',
                'author': 'test',
                'datetime': 'nope',
                'data': {
                    'address': 'test',
                    'traceback': 'test'
                }
            })

        reporting.check_report({
            'ipv4': '127.0.1.1',
            'hostname': 'nosetests',
            'type': 'something',
            'author': 'test',
            'datetime': utcnow(),
            'data': {
                'address': 'test',
                'traceback': 'test'
            }
        })

    async def test_002_exception(self):
        reporting.exception(Exception('nope'))
        # Troubles patching datetime.utcnow
        eq_(self.publisher.publish.call_count, 1)
        calls = self.publisher.publish
        call_arg = calls.call_args[0][0]
        assert_true('nope' in call_arg['data']['traceback'])
