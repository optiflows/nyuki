import asyncio
from asynctest import (
    TestCase, patch, Mock, CoroutineMock, ignore_loop, exhaust_callbacks, call
)
from nose.tools import assert_raises, eq_, assert_not_in, assert_in
from slixmpp import JID

from nyuki.bus.xmpp import _XmppClient, XmppBus
from nyuki.bus.persistence import EventStatus
from nyuki.bus.persistence.memory_backend import MemoryBackend


class TestBusClient(TestCase):

    def setUp(self):
        self.client = _XmppClient('test@localhost', 'password')

    @ignore_loop
    def test_001a_init(self):
        # Ensure bus client instanciation (method 1)
        host, port = self.client._address
        eq_(host, 'localhost')
        eq_(port, 5222)

    @ignore_loop
    def test_001b_init(self):
        # Ensure bus client instanciation (method 2)
        client = _XmppClient('test', 'password', '127.0.0.1', 5555)
        host, port = client._address
        eq_(host, '127.0.0.1')
        eq_(port, 5555)


class TestXmppBus(TestCase):

    def setUp(self):
        self.bus = XmppBus(Mock())
        self.bus.configure('test@localhost', 'password')

    @ignore_loop
    def test_001_muc_address(self):
        muc = self.bus._muc_address('topic')
        eq_(muc, 'topic@mucs.localhost')

    async def test_002_on_event(self):
        self.bus._connected.set()
        cb = CoroutineMock()
        with patch.object(self.bus._mucs, 'join_muc') as join_mock:
            await self.bus.subscribe('someone', cb)
            join_mock.assert_called_once_with('someone@mucs.localhost', 'test')
        msg = self.bus.client.Message()
        msg['type'] = 'groupchat'
        msg['to'] = JID('test@localhost')
        msg['from'] = JID('someone@localhost')
        msg['body'] = '{"key": "value"}'
        await self.bus._on_event(msg)
        cb.assert_called_once_with('someone', {'key': 'value'})

    @patch('slixmpp.xmlstream.stanzabase.StanzaBase.send')
    async def test_003a_publish(self, send_mock):
        self.bus._connected.set()
        asyncio.ensure_future(self.bus.publish({'message': '1'}))
        asyncio.ensure_future(self.bus.publish({'message': '2'}))
        asyncio.ensure_future(self.bus.publish({'message': '3'}))
        await exhaust_callbacks(self.loop)
        eq_(send_mock.call_count, 3)

    async def test_003b_publish_no_dict(self):
        with assert_raises(TypeError):
            await self.bus.publish('not a dict')

    @patch('slixmpp.xmlstream.stanzabase.StanzaBase.send')
    async def test_003c_publish_repr(self, send_mock):
        self.bus._connected.set()
        asyncio.ensure_future(self.bus.publish({'object': object()}))
        await exhaust_callbacks(self.loop)
        eq_(send_mock.call_count, 1)

    @patch('slixmpp.xmlstream.stanzabase.StanzaBase.send')
    async def test_003d_publish_unknown_topic(self, send_mock):
        self.bus._connected.set()
        with patch.object(self.bus._mucs, 'join_muc') as join_mock:
            asyncio.ensure_future(self.bus.publish({'test': 'message'}, 'unknown/topic'))
            await exhaust_callbacks(self.loop)
            join_mock.assert_called_once_with('unknown.topic@mucs.localhost', 'test')
        eq_(send_mock.call_count, 1)

    async def test_004_on_register_callback(self):
        with patch('slixmpp.stanza.Iq.send', new=CoroutineMock()) as send_mock:
            await self.bus._on_register(None)
            send_mock.assert_called_once_with()

    async def test_005_reconnect(self):
        self.bus.reconnect = True
        with patch.object(self.bus.client, '_connect_routine') as mock:
            await self.bus._on_disconnect(None)
            eq_(mock.call_count, 1)

    @patch('slixmpp.xmlstream.stanzabase.StanzaBase.send')
    async def test_006_direct_message(self, send_mock):
        self.bus._connected.set()
        await self.bus.send_message('yo', {'message': 'test'})
        send_mock.assert_called_once_with()

    async def test_007_on_direct_message(self):
        cb = CoroutineMock()
        self.bus.direct_subscribe(cb)
        msg = self.bus.client.Message()
        msg['type'] = 'message'
        msg['from'] = JID('other@localhost')
        msg['body'] = '{"key": "value"}'
        await self.bus._on_direct_message(msg)
        cb.assert_called_once_with('other', {'key': 'value'})

    async def stream_error(self):
        for uid in self.bus._publish_futures.keys():
            await self.bus._on_stream_error({'id': uid})

    async def publish_fail(self):
        for uid in self.bus._publish_futures.keys():
            await self.bus._on_failed_publish({
                'id': uid,
                'to': JID('someone@localhost'),
                'from': JID('test@localhost')
            })

    async def publish_ok(self):
        for uid in self.bus._publish_futures.keys():
            await self.bus._on_event({
                'id': uid, 'from': JID('test@localhost')
            })

    @patch('slixmpp.xmlstream.stanzabase.StanzaBase.send', Mock)
    @patch('nyuki.bus.XmppBus.subscribe')
    async def test_008_stream_error_resubscribe(self, submock):
        self.bus._connected.set()
        self.loop.call_later(0.1, asyncio.ensure_future, self.stream_error())
        self.loop.call_later(0.2, asyncio.ensure_future, self.publish_ok())
        await self.bus.publish({'one': 'two'})
        submock.assert_called_once_with('test', None)

    @patch('slixmpp.xmlstream.stanzabase.StanzaBase.send', Mock)
    @patch('nyuki.bus.XmppBus.subscribe')
    async def test_009_failed_publish_resubscribe(self, submock):
        self.bus._connected.set()
        self.loop.call_later(0.1, asyncio.ensure_future, self.publish_fail())
        self.loop.call_later(0.2, asyncio.ensure_future, self.publish_ok())
        await self.bus.publish({'one': 'two'})
        submock.assert_called_once_with('test', None)


class TestPersistence(TestCase):

    @patch('nyuki.bus.persistence.mongo_backend.MongoBackend.init')
    async def setUp(self, init):
        self.bus = XmppBus(Mock())
        self.bus.configure(
            'test@localhost', 'password',
            persistence={'backend': 'memory'}
        )
        self.backend = self.bus._persistence.backend

    @patch('slixmpp.xmlstream.stanzabase.StanzaBase.send', Mock)
    async def test_001_store_replay(self):
        await self.bus.publish({'something': 'something'})
        await self.bus.publish({'another': 'event'})

        # Backend received the events
        eq_(len(self.backend._last_events), 2)
        eq_(self.backend._last_events.list[0]['status'], EventStatus.FAILED.value)

        # Check replay send the same event
        event_0_uid = self.backend._last_events.list[0]['id']
        event_1_uid = self.backend._last_events.list[1]['id']
        with patch.object(self.bus, 'publish') as pub:
            await self.bus.replay()
            pub.assert_has_calls([
                call({'something': 'something'}, topic='test', previous_uid=event_0_uid),
                call({'another': 'event'}, topic='test', previous_uid=event_1_uid),
            ])

    def finish_publications(self):
        for future in self.bus._publish_futures.values():
            future.set_result(None)

    @patch('slixmpp.xmlstream.stanzabase.StanzaBase.send', Mock)
    async def test_002_store_xmpp_connected(self):
        self.bus._connected.set()
        self.loop.call_later(0.1, self.finish_publications)
        await self.bus.publish({'something': 'something'})
        eq_(len(self.backend._last_events), 1)
        eq_(self.backend._last_events.list[0]['status'], EventStatus.SENT.value)
