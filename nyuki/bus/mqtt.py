import asyncio
import json
import logging
import re
from collections import namedtuple
from copy import copy
from uuid import uuid4

from hbmqtt.client import MQTTClient, ConnectException, ClientException
from hbmqtt.errors import NoDataException
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2
from nyuki.services import Service
from nyuki.utils import serialize_object
from yarl import URL


log = logging.getLogger(__name__)

MQTTSubRegex = namedtuple('MQTTSubRegex', ['regex', 'callbacks'])


class MqttBus(Service):

    CONF_SCHEMA = {
        'type': 'object',
        'required': ['bus'],
        'properties': {
            'bus': {
                'type': 'object',
                'properties': {
                    'dsn': {'type': 'string', 'minLength': 1},
                    'cafile': {'type': 'string', 'minLength': 1},
                    'certfile': {'type': 'string', 'minLength': 1},
                    'keyfile': {'type': 'string', 'minLength': 1},
                    'keep_alive': {'type': 'integer', 'minimum': 1},
                    'ping_delay': {'type': 'integer', 'minimum': 1}
                },
                'additionalProperties': False
            }
        }
    }

    def __init__(self, nyuki, loop=None):
        self._nyuki = nyuki
        self._nyuki.register_schema(self.CONF_SCHEMA)
        self._loop = loop or asyncio.get_event_loop()
        self._dsn = None
        self.client = None
        self._subscriptions = {}
        self._regex_subscriptions = {}
        self._persisted = {}

        # Coroutines
        self.connect_future = None
        self.listen_future = None

    @property
    def topics(self):
        return list(self._subscriptions.keys())

    @property
    def name(self):
        return self._dsn.user

    def configure(self, dsn, cafile=None, certfile=None, keyfile=None,
                  keep_alive=60, ping_delay=5):
        self._dsn = URL(dsn)
        if self._dsn.scheme in ['mqtts', 'wss']:
            if not cafile or not certfile or not keyfile:
                raise ValueError(
                    "secured scheme requires 'cafile', 'certfile' and 'keyfile'"
                )

        self._cafile = cafile
        self.client = MQTTClient(
            client_id=self.name,
            config={
                'auto_reconnect': False,
                'certfile': certfile,
                'keyfile': keyfile,
                'keep_alive': keep_alive,
                'ping_delay': ping_delay
            },
            loop=self._loop
        )

    async def start(self):
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

    def _regex_topic(self, topic):
        """
        Transform the MQTT pattern into a regex object.
        """
        return re.compile(r'^{}$'.format(
            topic.replace('+', '[^\/]+').replace('#', '.+')
        ))

    async def subscribe(self, topic, callback):
        """
        Subscribe to a topic and setup the callback.
        This will pre-compile the regex used for this topic.
        """
        if not asyncio.iscoroutinefunction(callback):
            raise ValueError('event callback must be a coroutine')

        sub = False
        log.debug('MQTT subscription to %s -> %s', topic, callback.__name__)
        # Regexes are about topics like 'word/+/word' or 'word/#'
        is_regex = '+' in topic or topic.endswith('#')
        if is_regex is True:
            try:
                self._regex_subscriptions[topic].callbacks.add(callback)
            except KeyError:
                self._regex_subscriptions[topic] = MQTTSubRegex(
                    regex=self._regex_topic(topic),
                    callbacks={callback},
                )
                sub = True
        # Standard topics are a simple dict/set pair.
        else:
            try:
                self._subscriptions[topic].add(callback)
            except KeyError:
                self._subscriptions[topic] = {callback}
                sub = True

        # Send the subscription packet only if we were not subscribed yet
        if sub is True:
            await self.client.subscribe([(topic, QOS_1)])
            log.info('Subscribed to %s', topic)

        # Look for persisted message received prior to this subscription.
        if topic in self._persisted:
            for data in self._persisted[topic]:
                self._handle_message(topic, data)
            del self._persisted[topic]

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
        # Resubscribe in case the MQTT broker restarted.
        subs = [topic for topic in self._subscriptions.keys()]
        subs.extend(self._regex_subscriptions.keys())
        for topic in subs:
            log.info('Resubscribing to %s', topic)
            await self.client.subscribe([(topic, QOS_1)])

    async def publish_qos_0(self, data, topic):
        return await self.publish(data, topic, qos=QOS_0)

    async def publish_qos_1(self, data, topic):
        return await self.publish(data, topic, qos=QOS_1)

    async def publish_qos_2(self, data, topic):
        return await self.publish(data, topic, qos=QOS_2)

    async def publish(self, data, topic=None, qos=QOS_0):
        """
        Publish in given topic or default one
        """
        if not topic:
            topic = self.name

        log.debug("Publishing event to '%s': %s", topic, data)
        data = json.dumps(data, default=serialize_object)

        if self.client._connected_state.is_set():
            try:
                await self.client.publish(topic, data.encode(), qos=qos)
            except Exception as exc:
                log.error('Error while publishing: %s', exc)
            else:
                log.debug('Event successfully sent to topic %s', topic)
        else:
            log.error('Failed to send event to topic %s', topic)

    async def _run(self):
        """
        Handle reconnection every 3 seconds
        """
        while True:
            log.info('Trying MQTT connection to %s', self._dsn)
            try:
                await self.client.connect(
                    str(self._dsn),
                    cleansession=False,
                    cafile=self._cafile,
                )
            except (ConnectException, NoDataException) as exc:
                log.error(exc)
                log.info('Waiting 3 seconds to reconnect')
                await asyncio.sleep(3.0)
                continue

            log.info('Connection made with MQTT')
            # Start listening.
            await self._resubscribe()
            self.listen_future = asyncio.ensure_future(self._listen())
            # Ensure the _persisted dict is emptied at some point.
            self._loop.call_later(60, self._clear_persisted)
            # Blocks until mqtt is disconnected.
            await self.client._handler.wait_disconnect()
            # Clean listen_future.
            self.listen_future.cancel()
            self.listen_future = None

    def _clear_persisted(self):
        self._persisted = {}

    async def _listen(self):
        """
        Listen to events after a successful connection.
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

            self._handle_message(
                message.topic,
                json.loads(message.data.decode()),
            )

    def _handle_message(self, topic, data):
        handled = False
        # Iterate and call all regex topics callbacks
        for mqttregex in self._regex_subscriptions.values():
            if mqttregex.regex.match(topic):
                for callback in mqttregex.callbacks:
                    asyncio.ensure_future(callback(topic, data.copy()))
                handled = True

        # Iterate and call all single topic callbacks
        if topic in self._subscriptions:
            for callback in self._subscriptions[topic]:
                asyncio.ensure_future(callback(topic, data.copy()))
            handled = True

        # If the message was not linked to any known topic, keep it
        # in memory for a later subscription (should happen in an instant).
        # The dict is cleared 60 seconds after the connection.
        if handled is False:
            if topic in self._persisted:
                self._persisted[topic].append(data)
            else:
                self._persisted[topic] = [data]
