import json
import asyncio
import logging
import logging.config
from uuid import uuid4
from pijon import Pijon
from jsonschema import validate
from signal import SIGHUP, SIGINT, SIGTERM

from .api import Api
from .api.bus import ApiBusReplay, ApiBusTopics, ApiBusPublish
from .api.config import ApiConfiguration, ApiSwagger
from .bus import XmppBus, MqttBus, reporting
from .commands import get_command_kwargs
from .config import get_full_config, write_conf_json, merge_configs
from .debugging import StackSampler, ApiSampleEmitter
from .logs import DEFAULT_LOGGING
from .services import ServiceManager
from .discovery import Discovery
from .raft import RaftProtocol, ApiRaft
from .memory import Memory


log = logging.getLogger(__name__)


class Nyuki:

    """
    A lightweigh base class to build nyukis. A nyuki provides tools that shall
    help the developer with managing the following topics:
      - Bus of communication between nyukis.
      - Asynchronous events.
      - Capabilities exposure through a REST API.
    This class has been written to perform the features above in a reliable,
    single-threaded, asynchronous and concurrent-safe environment.
    The core engine of a nyuki implementation is the asyncio event loop
    (a single loop is used for all features).
    """

    # Configuration schema must follow jsonschema rules.
    BASE_CONF_SCHEMA = {
        'type': 'object',
        'required': ['log'],
        'properties': {
            'service': {'type': 'string', 'minLength': 1},
            'trace': {'type': 'boolean'},
        }
    }
    # API endpoints
    HTTP_RESOURCES = [
        ApiBusPublish,
        ApiBusReplay,
        ApiBusTopics,
        ApiConfiguration,
        ApiSwagger,
        ApiRaft,
        ApiSampleEmitter,
    ]

    def __init__(self, **kwargs):
        # List of configuration schemas
        self._schemas = []
        self._id = str(uuid4())[:8]

        # Initialize logging
        logging.config.dictConfig(DEFAULT_LOGGING)

        # Get configuration from multiple sources and register base schema
        kwargs = kwargs or get_command_kwargs()
        # Storing the optional init params, will be used when reloading
        self._launch_params = kwargs
        self._config_filename = kwargs.get('config')
        self._config = get_full_config(**kwargs)
        if self._config['log'] != DEFAULT_LOGGING:
            logging.config.dictConfig({
                **DEFAULT_LOGGING,
                **self._config['log']
            })
        self.register_schema(self.BASE_CONF_SCHEMA)

        # Setup stack sampling
        self._sampler = None
        self._set_stack_sampling()
        # Set loop
        self.loop = asyncio.get_event_loop() or asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self._services = ServiceManager(self)
        self._services.add('api', Api(self))

        # Add bus service if in conf file, xmpp (default) or mqtt
        bus_config = self._config.get('bus')
        if bus_config is not None:
            bus_service = bus_config.get('service', 'xmpp')
            if bus_service == 'xmpp':
                self._services.add('bus', XmppBus(self))
            elif bus_service == 'mqtt':
                self._services.add('bus', MqttBus(self))

        # Add NaaS (nyuki-as-a-service) related services
        if self._config.get('service'):
            # Discovery
            method = self._config.get('discovery', {}).get('method', 'dns')
            discovery = Discovery.get(method)
            self._services.add('discovery', discovery(self))
            # Raft
            self._services.add('raft', RaftProtocol(self))
            self.discovery.register(self.raft.discovery_handler)
            # Memory
            if self._config.get('memory'):
                self._services.add('memory', Memory(self))

        self.is_stopping = False

    def __getattribute__(self, name):
        """
        Getattr, or returns the specified service
        """
        try:
            return super().__getattribute__(name)
        except AttributeError as exc:
            try:
                return self._services.get(name)
            except KeyError:
                raise exc

    @property
    def id(self):
        return self._id

    @property
    def config(self):
        return self._config

    @property
    def reporter(self):
        """
        Ensure backwards compatibility
        TODO: must be removed
        """
        log.warning('Deprecated reporting call, use nyuki.bus.reporting')
        return reporting

    def _exception_handler(self, loop, context):
        if 'exception' not in context:
            log.warning('No exception in context: %s', context)
            return
        log.debug('Exception context: %s', context)

        exc = Exception("could not retrieve exception's traceback")
        if 'future' in context:
            try:
                context['future'].result()
            except Exception as e:
                exc = e
        else:
            exc = context['exception']

        reporting.exception(exc)

    def start(self):
        """
        Start the nyuki
        The nyuki process is terminated when this method is finished
        """
        self.loop.add_signal_handler(SIGTERM, self.abort, SIGTERM)
        self.loop.add_signal_handler(SIGINT, self.abort, SIGINT)
        self.loop.add_signal_handler(SIGHUP, self.hang_up, SIGHUP)

        # Configure services with nyuki's configuration
        log.debug('Running configure for services')
        for name, service in self._services.all.items():
            service.configure(**self._config.get(name, {}))
        log.debug('Done configuring')

        # Start services
        try:
            self.loop.run_until_complete(self._services.start())
        except Exception as exc:
            log.error(exc)
            return

        if 'bus' in self._services:
            self.bus.init_reporting()
            self.loop.set_exception_handler(self._exception_handler)

        # Call for setup
        if not asyncio.iscoroutinefunction(self.setup):
            log.warning('setup method must be a coroutine')
            self.setup = asyncio.coroutine(self.setup)
        self.loop.run_until_complete(self.setup())

        # Main loop
        self.loop.run_forever()

        # Call for teardown
        if not asyncio.iscoroutinefunction(self.teardown):
            log.warning('teardown method must be a coroutine')
            self.teardown = asyncio.coroutine(self.teardown)
        self.loop.run_until_complete(self.teardown())

        # Close everything : terminates nyuki
        self.loop.close()

    def _stop_loop(self):
        """
        Call the loop to stop itself.
        """
        self.loop.call_soon_threadsafe(self.loop.stop)

    def abort(self, signal):
        """
        Signal handler: gracefully stop the nyuki.
        """
        log.warning('Caught signal %d, stopping nyuki', signal)
        asyncio.ensure_future(self.stop())

    async def stop(self, timeout=5):
        """
        Stop the nyuki
        """
        if self.is_stopping:
            log.warning('Force closing the nyuki')
            self._stop_loop()
            return

        self.is_stopping = True
        if self._sampler:
            self._sampler.stop()
        await self._services.stop()
        self._stop_loop()

    def hang_up(self, signal):
        """
        Signal handler: reload the nyuki
        """
        log.warning('Caught signal %d, reloading the nyuki', signal)
        try:
            self._config = get_full_config(**self._launch_params)
        except json.decoder.JSONDecodeError as e:
            log.error(
                'Could not load the new configuration, '
                'fallback on the previous one: "%s"', e
            )
        else:
            asyncio.ensure_future(self._reload_config())

    def register_schema(self, schema, format_checker=None):
        """
        Add a jsonschema to validate on configuration update.
        """
        self._schemas.append((schema, format_checker))
        self._validate_config()

    def _validate_config(self, config=None):
        """
        Validate on all registered configuration schemas.
        """
        log.debug('Validating configuration')
        config = config or self._config
        for schema, checker in self._schemas:
            validate(config, schema, format_checker=checker)

    async def setup(self):
        """
        First thing called when starting the event loop, coroutine or not.
        """
        log.warning('Setup called, but not overridden')

    async def reload(self):
        """
        Called when the configuration is modified
        """
        log.warning('Reload called, but not overridden')

    async def teardown(self):
        """
        Called right before closing the event loop, stopping the Nyuki.
        """
        log.warning('Teardown called, but not overridden')

    async def free_slot(self):
        """
        Called when the bus memory buffer has free slots available.
        """
        log.warning('Buffer free slot callback not overridden')

    def update_config(self, *new_confs):
        """
        Update the current configuration with the given list of dicts.
        """
        config = merge_configs(self._config, *new_confs)
        self._validate_config(config)
        self._config = config

    def save_config(self):
        """
        Save the current configuration dict to its JSON file.
        """
        if self._config_filename:
            write_conf_json(self.config, self._config_filename)
        else:
            log.warning('Not saving the default read-only configuration file')

    def migrate_config(self):
        """
        Migrate configuration dict using pijon migrations.
        """
        tool = Pijon(load=False)
        current = self._config.get('version', 0)
        log.debug("Current configuration file version: {}".format(current))

        if not tool.migrations or current >= tool.last_migration:
            log.debug("Configuration is up to date")
            return

        log.warning("Configuration seems out of date, applying migrations")
        update = tool.migrate(self._config)
        self._validate_config(update)
        self._config = update
        self.save_config()

    async def _reload_config(self, request=None):
        """
        Reload the configuration and the services
        """
        logging.config.dictConfig(self._config['log'])
        self._set_stack_sampling()
        await self.reload()
        for name, service in self._services.all.items():
            if (request is not None and name in request) or request is None:
                await service.stop()
                service.configure(**self._config.get(name, {}))
                asyncio.ensure_future(service.start())

    def _set_stack_sampling(self):
        enable = self.config.get('trace') is True
        # Enable sampler trace
        if not self._sampler and enable:
            self._sampler = StackSampler()
            self._sampler.start()
        # Disable sampler trace if it is enabled
        elif self._sampler and not enable:
            self._sampler.stop()
            self._sampler = None
