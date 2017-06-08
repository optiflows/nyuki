import re
import json
import asyncio
import logging
from copy import copy
from uuid import uuid4
from collections import namedtuple
from hbmqtt.client import MQTTClient, ConnectException, ClientException
from hbmqtt.errors import NoDataException
from hbmqtt.mqtt.constants import QOS_1

from nyuki.bus import reporting
from nyuki.services import Service
from nyuki.utils import serialize_object
from .persistence import BusPersistence, EventStatus


log = logging.getLogger(__name__)


MQTTRegex = namedtuple('MQTTRegex', ['regex', 'callbacks'])


class MqttBus(Service):

    """
    Nyuki topics formatted as:
        - global publications:
            {nyuki_name}/publications
    """

    SERVICE = 'mqtt'
    CONF_SCHEMA = {
        'type': 'object',
        'required': ['bus'],
        'properties': {
            'bus': {
                'type': 'object',
                'required': ['name'],
                'properties': {
                    'cafile': {'type': 'string', 'minLength': 1},
                    'certfile': {'type': 'string', 'minLength': 1},
                    'host': {'type': 'string', 'minLength': 1},
                    'keyfile': {'type': 'string', 'minLength': 1},
                    'name': {'type': 'string', 'minLength': 1},
                    'port': {'type': 'integer'},
                    'persistence': {
                        'type': 'object',
                        'required': ['backend'],
                        'properties': {
                            'backend': {'type': 'string', 'enum': [
                                'memory',
                                'mongo',
                            ]},
                            'host': {'type': 'string'},
                            'ttl': {'type': 'number'},
                        },
                    },
                    'scheme': {
                        'type': 'string',
                        'enum': ['ws', 'wss', 'mqtt', 'mqtts']
                    },
                    'service': {'type': 'string', 'minLength': 1},
                    'keep_alive': {'type': 'integer', 'minimum': 1},
                    'ping_delay': {'type': 'integer', 'minimum': 1}
                },
                'additionalProperties': False
            }
        }
    }

    def __init__(self, nyuki, loop=None):
        self._nyuki = nyuki
        nyuki.register_schema(self.CONF_SCHEMA)
        self._loop = loop or asyncio.get_event_loop()
        self._host = None
        self.client = None
        self._pending = {}
        self.name = None
        self._subscriptions = {}
        self._regex_subscriptions = {}

        # Coroutines
        self.connect_future = None
        self.listen_future = None

    @property
    def topics(self):
        return list(self._subscriptions.keys())

    def configure(self, name, scheme='mqtt', host='localhost', port=1883,
                  cafile=None, certfile=None, keyfile=None, persistence={},
                  service=None, keep_alive=60, ping_delay=5):
        if scheme in ['mqtts', 'wss']:
            if not cafile or not certfile or not keyfile:
                raise ValueError(
                    "secured scheme requires 'cafile', 'certfile' and 'keyfile'"
                )

        self._host = '{}://{}:{}'.format(scheme, host, port)
        self.name = name
        self._cafile = cafile
        self.client = MQTTClient(
            config={
                'auto_reconnect': False,
                'certfile': certfile,
                'keyfile': keyfile,
                'keep_alive': keep_alive,
                'ping_delay': ping_delay
            },
            loop=self._loop
        )

        # Persistence storage
        if persistence:
            self._persistence = BusPersistence(name=name, **persistence)
            log.info('Bus persistence set to %s', self._persistence.backend)
        else:
            self._persistence = None

    async def start(self):
        if self._persistence:
            await self._persistence.init()

        def cancelled(future):
            try:
                future.result()
            except asyncio.CancelledError:
                log.debug('future cancelled: %s', future)
        self.connect_future = asyncio.ensure_future(self._run())
        self.connect_future.add_done_callback(cancelled)

    async def stop(self):
        # Clean client
        if self.client is not None:
            for task in self.client.client_tasks:
                log.debug('cancelling mqtt client tasks')
                task.cancel()
            if self.client._connected_state.is_set():
                log.debug('disconnecting mqtt client')
                await self.client.disconnect()
        # Clean tasks
        if self.connect_future:
            log.debug('cancelling _run coroutine')
            self.connect_future.cancel()
        if self.listen_future:
            log.debug('cancelling _listen coroutine')
            self.listen_future.cancel()
        log.info('MQTT service stopped')

    def init_reporting(self):
        """
        Initialize reporting module
        """
        reporting.init(self.name, self)

    def _regex_topic(self, topic):
        """
        Transform the MQTT pattern into a regex object.
        """
        return re.compile(r'^{}$'.format(
            topic.replace('+', '[^\/]+').replace('#', '.+')
        ))

    async def replay(self, since=None, status=None):
        """
        Replay events since the given datetime (or all if None)
        """
        if not self._persistence:
            return

        msg = 'Replaying events'
        if since:
            msg += ' since {}'.format(since)
        if status:
            msg += ' with status {}'.format(status)
        log.info(msg)

        events = await self._persistence.retrieve(since, status)
        for event in events:
            # Publish events one by one in the right publish time order
            await self.publish(json.loads(
                event['message']),
                event['topic'],
                event['id']
            )

    async def subscribe(self, topic, callback):
        """
        Subscribe to a topic and setup the callback.
        This will pre-compile the regex used for this topic.
        """
        if not asyncio.iscoroutinefunction(callback):
            raise ValueError('event callback must be a coroutine')

        log.debug('MQTT subscription to %s -> %s', topic, callback.__name__)
        await self.client.subscribe([(topic, QOS_1)])

        # Regexes are about topics like 'word/+/word' or 'word/#'
        is_regex = '+' in topic or topic.endswith('#')
        if is_regex is True:
            try:
                self._regex_subscriptions[topic].callbacks.add(callback)
            except KeyError:
                self._regex_subscriptions[topic] = MQTTRegex(
                    regex=self._regex_topic(topic),
                    callbacks={callback},
                )
        # Standard topics are a simple dict/set pair.
        else:
            try:
                self._subscriptions[topic].add(callback)
            except KeyError:
                self._subscriptions[topic] = {callback}

        log.info('Subscribed to %s', topic)

    async def _unsub_regex(self, topic, callback):
        """
        Unsubscribe from a regex topic.
        """
        if topic not in self._regex_subscriptions:
            return
        if callback in self._regex_subscriptions[topic].callbacks:
            log.debug(
                'MQTT unsubscription from %s -> %s',
                topic, callback.__name__,
            )
            self._regex_subscriptions[topic].callbacks.remove(callback)
        if callback is None or not self._regex_subscriptions[topic].callbacks:
            del self._regex_subscriptions[topic]
            await self.client.unsubscribe([topic])
            log.info('Unsubscribed from %s', topic)

    async def _unsub(self, topic, callback):
        """
        Unsubscribe from a standard topic.
        """
        if topic not in self._subscriptions:
            return
        if callback in self._subscriptions[topic]:
            log.debug(
                'MQTT unsubscription from %s -> %s',
                topic, callback.__name__,
            )
            self._subscriptions[topic].remove(callback)
        if callback is None or not self._subscriptions[topic]:
            del self._subscriptions[topic]
            await self.client.unsubscribe([topic])
            log.info('Unsubscribed from %s', topic)

    async def unsubscribe(self, topic, callback=None):
        """
        Unsubscribe from a topic, remove callback if set.
        """
        if '+' in topic or topic.endswith('#'):
            await self._unsub_regex(topic, callback)
        else:
            await self._unsub(topic, callback)

    async def _resubscribe(self):
        """
        Resubscribe on reconnection.
        """
        subs = list(self._subscriptions.keys()) + \
            list(self._regex_subscriptions.keys())
        for topic in subs:
            log.debug('Resubscribing to %s', topic)
            await self.client.subscribe([(topic, QOS_1)])

    async def publish(self, data, topic=None, previous_uid=None):
        """
        Publish in given topic or default one
        """
        uid = previous_uid or str(uuid4())
        topic = topic or self.name
        log.info('Publishing an event to %s', topic)
        log.debug('dump: %s', data)
        data = json.dumps(data, default=serialize_object)

        if self.client._connected_state.is_set():
            try:
                await self.client.publish(topic, data.encode())
            except Exception as exc:
                status = EventStatus.PENDING
                log.error('Error while publishing: %s', exc)
            else:
                status = EventStatus.SENT
                log.debug('Event successfully sent to topic %s', topic)
        else:
            status = EventStatus.FAILED
            log.error('Failed to send event to topic %s', topic)

        if self._persistence:
            if previous_uid is None:
                # This event was not previously sent
                await self._persistence.store({
                    'id': uid,
                    'status': status.value,
                    'topic': topic,
                    'message': data,
                })
            else:
                await self._persistence.update(uid, status)

    async def _run(self):
        """
        Handle reconnection every 3 seconds
        """
        while True:
            log.info('Trying MQTT connection to %s', self._host)
            try:
                await self.client.connect(self._host, cafile=self._cafile)
            except (ConnectException, NoDataException) as exc:
                log.error(exc)
                log.info('Waiting 3 seconds to reconnect')
                await asyncio.sleep(3.0)
                continue

            # Replaying events
            log.info('Connection made with MQTT')
            if self._persistence:
                asyncio.ensure_future(self.replay(
                    status=EventStatus.not_sent()
                ))

            # Start listening
            await self._resubscribe()
            self.listen_future = asyncio.ensure_future(self._listen())
            # Blocks until mqtt is disconnected
            await self.client._handler.wait_disconnect()
            # Clean listen_future
            self.listen_future.cancel()
            self.listen_future = None

    async def _listen(self):
        """
        Listen to events after a successful connection
        """
        while True:
            try:
                message = await self.client.deliver_message()
            except ClientException as exc:
                log.error(exc)
                break

            if message is None:
                log.info('listening loop ended')
                break

            topic = message.topic
            data = json.loads(message.data.decode())

            # Iterate and call all regex topics callbacks
            for mqttregex in self._regex_subscriptions.values():
                if mqttregex.regex.match(topic):
                    log.debug("Event from topic '%s': %s", topic, data)
                    for callback in mqttregex.callbacks:
                        asyncio.ensure_future(callback(topic, copy(data)))

            try:
                # Iterate and call all single topic callbacks
                for callback in self._subscriptions[topic]:
                    asyncio.ensure_future(callback(topic, copy(data)))
            except KeyError:
                pass
