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

    async def test_001_exception(self):
        reporting.exception(Exception('nope'))
        # Troubles patching datetime.utcnow
        eq_(self.publisher.publish.call_count, 1)
        calls = self.publisher.publish
        call_arg = calls.call_args[0][0]
        assert_true('nope' in call_arg['data']['traceback'])
