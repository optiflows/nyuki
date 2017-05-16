import asyncio
import json
import logging
from slixmpp import ClientXMPP
from slixmpp.exceptions import IqError, IqTimeout
from uuid import uuid4

from nyuki.bus import reporting
from nyuki.services import Service
from nyuki.utils import serialize_object

from .persistence import BusPersistence, EventStatus, PersistenceError


log = logging.getLogger(__name__)


class PublishError(Exception):
    pass


class _XmppClient(ClientXMPP):

    """
    XMPP client to connect to the bus.
    This class is based on Slixmpp (fork of Sleexmpp) using asyncio event loop.
    """

    def __init__(self, jid, password, host=None, port=5222, certificate=None):
        super().__init__(jid, password)
        host = host or self.boundjid.domain
        self._address = (host, port)

        self.register_plugin('xep_0045')  # Multi-user chat
        self.register_plugin('xep_0077')  # In-band registration

        # Disable IPv6 until we really need it
        self.use_ipv6 = False

        if certificate:
            self.ca_certs = certificate

    def connect(self, **kwargs):
        """
        Connect to the XMPP server using the default address computed at init
        if not passed as argument.
        """
        try:
            self._address = kwargs.pop('address')
        except KeyError:
            pass
        super().connect(address=self._address, **kwargs)
        self.init_plugins()

    def start_tls(self):
        try:
            return super().start_tls()
        except FileNotFoundError:
            log.error('SSL certificates missing at %s', self.ca_certs)
            self.abort()

    def exception(self, exc):
        """
        xmlstream.py catches exceptions itself, calls this method afterwards
        """
        reporting.exception(exc)


class XmppBus(Service):

    """
    A simple class that implements Publish/Subscribe-like communications over
    XMPP. This communication layer is also called "bus".
    Each nyuki has its own "topic" (actually a MUC) to publish events. And it
    can publish events to that topic only. In the meantime, each nyuki can
    subscribe to any other topics to process events from other nyukis.
    """

    SERVICE = 'xmpp'
    CONF_SCHEMA = {
        "type": "object",
        "required": ["bus"],
        "properties": {
            "bus": {
                "type": "object",
                "required": ["jid", "password"],
                "properties": {
                    "certificate": {"type": "string", "minLength": 1},
                    "host": {"type": "string", "minLength": 1},
                    "jid": {"type": "string", "minLength": 1},
                    "muc_domain": {"type": ["string", "null"]},
                    "password": {"type": "string", "minLength": 1},
                    "persistence": {
                        "type": "object",
                        "properties": {
                            "backend": {
                                "type": "string",
                                "minLength": 1
                            }
                        }
                    },
                    "port": {"type": "integer"},
                    "service": {"type": "string", "minLength": 1}
                },
                "additionalProperties": False
            }
        }
    }

    def __init__(self, nyuki):
        self._nyuki = nyuki
        self._nyuki.register_schema(self.CONF_SCHEMA)
        self._loop = self._nyuki.loop or asyncio.get_event_loop()

        self.client = None

        # Connectivity stuff
        self.reconnect = False
        self._connected = asyncio.Event()
        self._persistence = None
        self._publish_futures = dict()

        # MUCs
        self.name = None
        self._callbacks = dict()
        self._mucs = None
        self._muc_domain = None

    @property
    def topics(self):
        if not self._mucs:
            return []
        return [topic.split('@')[0] for topic in self._mucs.rooms.keys()]

    async def start(self, timeout=0):
        self.client.connect()
        if timeout:
            await asyncio.wait_for(self._connected.wait(), timeout)

        try:
            await self._persistence.init()
        except PersistenceError:
            log.error(
                'Could not init persistence storage with %s',
                self._persistence.backend
            )

    def configure(self, jid, password, host='localhost', port=5222,
                  muc_domain='mucs.localhost', certificate=None,
                  persistence={}, service=None):
        # XMPP client and main handlers
        self.client = _XmppClient(
            jid, password, host, port, certificate=certificate
        )
        self.client.loop = self._loop
        self.client.add_event_handler('connection_failed', self._on_failure)
        self.client.add_event_handler('disconnected', self._on_disconnect)
        self.client.add_event_handler('message', self._on_direct_message)
        self.client.add_event_handler('register', self._on_register)
        self.client.add_event_handler('session_start', self._on_start)
        self.client.add_event_handler('stream_error', self._on_stream_error)

        # MUCs
        self.name = self.client.boundjid.user
        self._callbacks[self.name] = None
        self._mucs = self.client.plugin['xep_0045']
        self._muc_domain = muc_domain
        self.client.add_event_handler('groupchat_message', self._on_event)
        self.client.add_event_handler(
            'muc::{}::message_error'.format(self._muc_address(self.name)),
            self._on_failed_publish
        )

        # Persistence storage
        if persistence:
            self._persistence = BusPersistence(name=self.name, **persistence)
            log.info('Bus persistence set to %s', self._persistence.backend)
        else:
            self._persistence = None

    async def stop(self):
        if not self.client:
            return

        if not self.client.transport:
            log.warning('XMPP client is already disconnected')
            return

        self.reconnect = False
        self.client.disconnect(wait=2)

        try:
            await asyncio.wait_for(self.client.disconnected, 2.0)
        except asyncio.TimeoutError:
            log.error('Could not end bus connection after 2 seconds')

    def init_reporting(self):
        """
        Initialize reporting module
        """
        reporting.init(self.client.boundjid.user, self)

    def _muc_address(self, topic):
        return '{}@{}'.format(topic, self._muc_domain)

    async def _on_register(self, event):
        """
        XMPP event handler while registering a user (in-band registration).
        """
        resp = self.client.Iq()
        resp['type'] = 'set'
        resp['register']['username'] = self.client.boundjid.user
        resp['register']['password'] = self.client.password

        try:
            await resp.send()
        except IqError as exc:
            error = exc.iq['error']['text']
            log.debug("Could not register account: {}".format(error))
        except IqTimeout:
            log.error("No response from the server")
        else:
            log.debug("Account {} created".format(self.client.boundjid))

    async def _on_start(self, event):
        """
        XMPP event handler when the connection has been made.
        """
        log.info('Connected to XMPP server at {}:{}'.format(
            *self.client._address
        ))
        self.reconnect = True
        self.client.send_presence()
        self.client.get_roster()

        # Auto-subscribe to the topic where the nyuki could publish events.
        self._connected.set()
        # Reconnect to all channels (+ `self.name`, set up in configure())
        await self._resubscribe()

        # Replay events that have been lost, if any
        if self._persistence:
            await self.replay(status=EventStatus.FAILED, wait=3)

    async def _on_disconnect(self, event):
        """
        XMPP event handler when the client has been disconnected.
        """
        if self._connected.is_set():
            log.warning('Disconnected from the bus')
            self._connected.clear()

        for future in self._publish_futures.values():
            future.cancel()

        if self.reconnect:
            # Restart the connection loop
            await self.client._connect_routine()

    def _on_failure(self, event):
        """
        XMPP event handler when something is going wrong with the connection.
        """
        log.error('Connection to XMPP server {}:{} failed'.format(
            *self.client._address
        ))
        self.client.abort()

    async def _on_stream_error(self, event):
        """
        Received on stream_error from prosody
        (TODO: event informations unknown)
        """
        log.error('Received unexpected stream error, resubscribing')
        log.debug('event dump: %s', event)
        await self._resubscribe()
        if event['id'] in self._publish_futures:
            if not self._publish_futures[event['id']].done():
                self._publish_futures[event['id']].set_exception(PublishError)
            else:
                log.debug('Publish future was already done')

    async def _on_failed_publish(self, event):
        """
        Event received when a publish has failed
        (publish informations in the event)
        """
        uid = event['id']
        muc = event['from'].user
        log.warning("Was not subscribed to MUC '%s', retrying", muc)
        await self.subscribe(muc, self._callbacks.get(muc))
        if not self._publish_futures[uid].done():
            self._publish_futures[uid].set_exception(PublishError)
        else:
            log.debug('Publish future was already done for id %s', uid)

    async def _on_event(self, event):
        """
        XMPP event handler when a Nyuki Event has been received.
        """
        # Ignore events from the nyuki itself
        # Event's resource is checked as well in case of self monitoring:
        # 'monitoring@mucs.localhost/this_nyuki'
        efrom = event['from'].user
        if efrom == self.name or event['from'].resource == self.name:
            future = self._publish_futures.get(event['id'])
            if not future:
                log.warning('Received own publish that was not in memory')
            else:
                future.set_result(None)
            return

        log.debug('event received: {}'.format(event))

        try:
            body = json.loads(event['body'])
        except ValueError:
            body = {}

        callback = self._callbacks.get(efrom)
        if callback:
            log.debug('calling callback %s', callback)
            if not asyncio.iscoroutinefunction(callback):
                log.warning('event callbacks must be coroutines')
                callback = asyncio.coroutine(callback)
            await callback(efrom, body)
        else:
            log.warning('No callback set for event from %s', efrom)

    async def replay(self, since=None, status=None, wait=0):
        """
        Replay events since the given datetime (or all if None)
        """
        log.info('Replaying events')
        if since:
            log.info('    since %s', since)
        if status:
            log.info('    with status %s', status)

        for future in self._publish_futures.values():
            future.cancel()

        if wait:
            log.info('Waiting %d seconds', wait)
            await asyncio.sleep(wait)

        events = await self._persistence.retrieve(since, status)
        for event in events:
            # Publish events one by one in the right publish time order
            await self.publish(
                json.loads(event['message']),
                topic=event['topic'],
                previous_uid=event['id']
            )

    async def publish(self, event, topic=None, previous_uid=None):
        """
        Send an event in the nyuki's own MUC so that other nyukis that joined
        the MUC can process it.
        """
        if not isinstance(event, dict):
            raise TypeError('Message must be a dict')
        if self._muc_domain is None:
            log.error('No subscription to any muc')
            return
        if topic is not None:
            # XMPP does not handle MUCs containing '/'
            topic = topic.replace('/', '.')
        if topic is not None and topic not in self.topics:
            # Automatically subscribe if required
            await self.subscribe(topic)

        msg = self.client.Message()
        msg['id'] = uid = previous_uid or str(uuid4())
        msg['type'] = 'groupchat'
        msg['to'] = self._muc_address(topic or self.name)
        msg['body'] = json.dumps(event, default=serialize_object)

        self._publish_futures[uid] = asyncio.Future()
        status = EventStatus.PENDING

        # Store the event as PENDING if it is new
        if self._persistence and previous_uid is None:
            await self._persistence.store({
                'id': uid,
                'status': status.value,
                'topic': topic or self.name,
                'message': msg['body'],
            })

        # Publish in MUC
        if self._connected.is_set():
            log.debug(">> publishing to '{}': {}".format(topic, event))
            log.info('Publishing an event to %s', msg['to'])
            status = None
            while True:
                msg.send()
                try:
                    await asyncio.wait_for(self._publish_futures[uid], 10.0)
                except asyncio.TimeoutError:
                    log.warning('Publication timed out, disconnecting slixmpp')
                    status = EventStatus.FAILED
                    # Crash connection and try to reconnect
                    self.reconnect = True
                    self.client.abort()
                    break
                except asyncio.CancelledError:
                    log.warning('Publication cancelled due to disconnection')
                    status = EventStatus.FAILED
                    break
                except PublishError:
                    status = EventStatus.FAILED
                    self._publish_futures[uid] = asyncio.Future()
                else:
                    log.info("Event successfully sent to MUC '%s'", msg['to'])
                    status = EventStatus.SENT
                    break
        else:
            status = EventStatus.FAILED

        del self._publish_futures[uid]
        # Once we have a result, update the stored event
        if self._persistence:
            await self._persistence.update(previous_uid or uid, status)

    async def _resubscribe(self):
        """
        Resubscribe everywhere
        """
        for topic in self._callbacks.keys():
            await self.subscribe(topic, self._callbacks.get(topic))

    async def subscribe(self, topic, callback=None):
        """
        Enter into a new chatroom using xep_0045.
        Room address format must be like '{topic}@mucs.localhost'
        """
        if not self._connected.is_set():
            log.warning("Waiting for a connection to subscribe to '%s'", topic)
        await self._connected.wait()

        # '/' are not supported in MUCs name
        topic = topic.replace('/', '.')
        self._mucs.join_muc(self._muc_address(topic), self.name)
        log.info("Subscribed to '%s'", topic)
        if self._callbacks.get(topic) is None:
            self._callbacks[topic] = callback
        else:
            log.warning("Callback already set for topic: %s", topic)

    async def unsubscribe(self, topic):
        """
        Leave a chatroom.
        """
        if not self._connected.is_set():
            log.warning("Waiting for a connection to unsubscribe from '%s'", topic)
        await self._connected.wait()

        self._mucs.leaveMUC(self._muc_address(topic), self.name)
        if topic in self._callbacks:
            del self._callbacks[topic]
        log.info("Unsubscribed from '%s'", topic)

    def direct_subscribe(self, callback):
        """
        Enable handling of direct messages from other nyukis
        """
        if not asyncio.iscoroutinefunction(callback):
            log.warning('event callbacks must be coroutines')
            callback = asyncio.coroutine(callback)
        self._direct_message_callback = callback
        log.info("Subscribed to direct messages")

    def direct_unsubscribe(self, source):
        """
        Remove the callback for direct messages
        """
        self._direct_message_callback = None
        log.info('Unsubscribed from direct messages')

    async def _on_direct_message(self, event):
        """
        Direct message events end up here
        """
        if event.get('type') == 'groupchat':
            return

        log.debug('direct message received: {}'.format(event))
        efrom = event['from'].user

        try:
            body = json.loads(event['body'])
        except ValueError:
            body = {}

        if self._direct_message_callback:
            log.debug('calling direct message callback')
            await self._direct_message_callback(efrom, body)
        else:
            log.warning('No callback set for direct messages')

    async def send_message(self, recipient, data):
        """
        Send a direct message to 'recipient'
        """
        if not recipient:
            log.debug('No recipient for direct message, ignoring it')
            return
        if not isinstance(data, dict):
            raise TypeError('Message must be a dict')
        if not self._connected.is_set():
            log.info("Waiting for a connection to direct message '%s'", recipient)
        await self._connected.wait()
        log.debug(">> direct message to '{}': {}".format(recipient, data))
        body = json.dumps(data)
        self.client.send_message(recipient, body)
