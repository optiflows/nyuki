import asyncio
import logging


log = logging.getLogger(__name__)


class Service(object):

    async def start(self, *args, **kwargs):
        raise NotImplementedError

    def configure(self, *args, **kwargs):
        raise NotImplementedError

    async def stop(self, *args, **kwargs):
        raise NotImplementedError


class ServiceManager(object):

    def __init__(self, nyuki):
        self._loop = nyuki.loop or asyncio.get_event_loop()
        self._nyuki = nyuki
        self.services = dict()
        self._running = False

    def __iter__(self):
        return self.services.__iter__()

    def add(self, name, service):
        """
        Add a managed service
        """
        if not isinstance(service, Service):
            raise TypeError('service must be an instance of Service')

        self.services[name] = service
        # Run the service immetiately if the others are running
        if self._running:
            service.configure(**self._nyuki.config.get(name, {}))
            asyncio.ensure_future(service.start())

    @property
    def all(self):
        return self.services

    def get(self, name):
        return self.services[name]

    async def start(self):
        """
        Start all services
        """
        tasks = [
            asyncio.ensure_future(service.start())
            for service in self.services.values()
        ]

        # Start all services, raise any exception
        log.info('Starting services: %s', ', '.join(self.services.keys()))
        log.debug('Running start tasks : %s', tasks)
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for f in done:
            f.result()

        self._running = True
        log.debug('Start tasks done')

    async def stop(self, timeout=5):
        """
        Stop all services with the given timeout
        """
        self._running = False
        tasks = [
            asyncio.ensure_future(s.stop())
            for s in self.services.values()
        ]

        done, not_done = await asyncio.wait(tasks, timeout=timeout)
        if not_done:
            log.warning('Could not stop services after %d seconds', timeout)
            log.debug('stop task not done : %s', not_done)
