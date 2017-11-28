from asynctest import TestCase, patch, ignore_loop, exhaust_callbacks, Mock
import json
from jsonschema import ValidationError
import os
from nose.tools import eq_, assert_true, assert_not_equal, assert_raises
import tempfile

from nyuki import Nyuki
from nyuki.api.config import ApiConfiguration
from nyuki.config import DEFAULT_CONF_FILE


@patch('nyuki.config.DEFAULT_CONF_FILE', tempfile.mkstemp()[1])
class TestNyuki(TestCase):

    def setUp(self):
        self.default = DEFAULT_CONF_FILE
        with open(self.default, 'w') as f:
            f.write('{"bus": {"name": "test"}}')
        kwargs = {'config': ''}
        self.nyuki = Nyuki(**kwargs)

        self.apiconf = ApiConfiguration()
        self.apiconf.nyuki = self.nyuki

    def tearDown(self):
        os.remove(self.default)

    @ignore_loop
    def test_001_update_config(self):
        assert_not_equal(self.nyuki.config['bus']['name'], 'new_name')
        self.nyuki.update_config({'bus': {'name': 'new_name'}})
        eq_(self.nyuki.config['bus']['name'], 'new_name')

        # Check read-only
        self.nyuki.save_config()
        with open(self.default, 'r') as f:
            eq_(f.read(), '{"bus": {"name": "test"}}')

    @ignore_loop
    def test_003_get_rest_configuration(self):
        response = self.apiconf.get(None)
        eq_(json.loads(bytes.decode(response.body)), self.nyuki._config)

    @patch('nyuki.bus.MqttBus.stop')
    async def test_004_patch_rest_configuration(self, bus_stop_mock):
        req = Mock()
        async def json():
            return {
                'bus': {'name': 'new_name'},
                'new': True
            }
        req.headers = {'Content-Type': 'application/json'}
        req.json = json
        await self.apiconf.patch(req)
        eq_(self.nyuki._config['new'], True)
        eq_(self.nyuki._config['bus']['name'], 'new_name')
        # finish coroutines
        await exhaust_callbacks(self.loop)
        bus_stop_mock.assert_called_once_with()

    @ignore_loop
    def test_005a_custom_schema_fail(self):
        with assert_raises(ValidationError):
            self.nyuki.register_schema({
                'type': 'object',
                'required': ['port'],
                'properties': {
                    'port': {
                        'type': 'integer',
                    }
                }
            })

    @ignore_loop
    def test_005b_custom_schema_ok(self):
        self.nyuki._config['port'] = 4000
        self.nyuki.register_schema({
            'type': 'object',
            'required': ['port'],
            'properties': {
                'port': {'type': 'integer'}
            }
        })
        # Base + API + Bus + custom
        eq_(len(self.nyuki._schemas), 4)

    async def test_005_stop(self):
        with patch.object(self.nyuki._services, 'stop') as mock:
            # Do not really close the loop as it would break other tests
            with patch.object(self.nyuki, '_stop_loop'):
                await self.nyuki.stop()
            mock.assert_called_once_with()
        assert_true(self.nyuki.is_stopping)


class TestNyukiWithConfig(TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.default = tempfile.mkstemp()[1]

    def tearDown(self):
        os.remove(self.default)

    @ignore_loop
    def test_001_copy_default(self):
        # Default conf file
        with open(self.default, 'w') as f:
            f.write('{"bus": {"name": "test"}}')

        # Our conf file does not exist yet
        conf = os.path.join(self.dir.name, 'myconf.json')
        with patch('nyuki.config.DEFAULT_CONF_FILE', self.default):
            kwargs = {'config': conf}
            self.nyuki = Nyuki(**kwargs)

        # Check our conf is created from default
        with open(conf, 'r') as f:
            eq_(f.read(), '{"bus": {"name": "test"}}')

    @ignore_loop
    def test_002_bad_conf_file(self):
        conf = os.path.join(self.dir.name, 'myconf.json')
        with open(conf, 'w') as f:
            f.write('{"bus": {"name": "test"')

        with assert_raises(ValueError):
            kwargs = {'config': conf}
            self.nyuki = Nyuki(**kwargs)


class TestNyukiNoDefault(TestCase):

    @ignore_loop
    def test_001_missing_default_file(self):
        with assert_raises(FileNotFoundError):
            Nyuki(**{'config': ''})
