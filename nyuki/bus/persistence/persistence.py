import asyncio
import logging

from nyuki.utils import utcnow
from nyuki.bus import reporting
from nyuki.bus.persistence.backend import PersistenceBackend
from nyuki.bus.persistence.events import EventStatus
from nyuki.bus.persistence.mongo_backend import MongoBackend


log = logging.getLogger(__name__)


class PersistenceError(Exception):
    pass


class FIFOSizedQueue(object):

    def __init__(self, size):
        self._list = list()
        self._size = size
        self._free_slot = asyncio.Event()
        self._free_slot.set()

    def __len__(self):
        return len(self._list)

    @property
    def size(self):
        return self._size

    @property
    def list(self):
        return self._list

    @property
    def is_full(self):
        return len(self._list) >= self._size

    @property
    def free_slot(self):
        return self._free_slot

    def put(self, item):
        while self.is_full:
            log.debug('queue full (%d), poping first item', len(self._list))
            self._list.pop(0)
        self._list.append(item)
        if self.is_full:
            self._free_slot.clear()

    def pop(self):
        item = self._list.pop(0)
        self._free_slot.set()
        return item

    def empty(self):
        while self._list:
            yield self.pop()


class BusPersistence(object):

    """
    This module will enable local caching for bus events to replace the
    current asyncio cache which is out of our control. (cf internal NYUKI-59)
    """

    FEED_DELAY = 5

    def __init__(self, backend=None, memory_size=None, loop=None, **kwargs):
        """
        TODO: mongo is the only one yet, we should parse available modules
              named `*_backend.py` and select after the given backend.
        """
        self._loop = loop or asyncio.get_event_loop()
        self._last_events = FIFOSizedQueue(memory_size or 10000)
        self.backend = None
        self._feed_future = None

        if not backend:
            log.info('No persistence backend selected, in-memory only')
            return

        if backend != 'mongo':
            raise ValueError("'mongo' is the only available backend")

        self.backend = MongoBackend(**kwargs)
        if not isinstance(self.backend, PersistenceBackend):
            raise PersistenceError('Wrong backend selected: {}'.format(backend))
        self._feed_future = asyncio.ensure_future(self._feed_backend())

    @property
    def memory_buffer(self):
        return self._last_events

    async def close(self):
        if self._feed_future:
            self._feed_future.cancel()
            await self._feed_future

    async def _feed_backend(self):
        """
        Periodically check connection to backend and dump in-memory events
        into it
        """
        while True:
            try:
                await asyncio.sleep(self.FEED_DELAY)
            except asyncio.CancelledError:
                log.debug('_feed_backend cancelled')
                await self._empty_last_events()
                break

            if not self._last_events.list:
                continue

            await self._empty_last_events()

    async def _empty_last_events(self):
        if await self.backend.ping():
            if self._last_events.list:
                log.info('Dumping all event into backend')
            try:
                for event in self._last_events.empty():
                    await self.backend.store(event)
            except Exception as exc:
                reporting.exception(exc)
        else:
            log.warning('No connection to backend to empty in-memory events')

    async def init(self):
        """
        Init backend
        """
        if self.backend:
            try:
                return await self.backend.init()
            except Exception as exc:
                raise PersistenceError from exc

    async def ping(self):
        """
        Connection check
        """
        if self.backend:
            try:
                return await asyncio.wait_for(self.backend.ping(), 2.0)
            except asyncio.TimeoutError:
                log.error('Timeout pinging backend')
                return False

    async def store(self, event):
        """
        Store a bus event from
        {
            "id": "uuid4",
            "status": "EventStatus.value",
            "topic": "muc",
            "message": "json dump"
        }
        adding a 'created_at' key.
        """
        log.debug("New event stored with uid '%s'", event['id'])
        event['created_at'] = utcnow()
        self._last_events.put(event)

    async def update(self, uid, status):
        """
        Update the status of a stored event
        """
        log.debug("Updating status of event '%s' to '%s'", uid, status)
        for event in self._last_events.list:
            if event['id'] == uid:
                event['status'] = status.value
                return

        log.debug('event not found in memory, checking backend')

        if self.backend:
            async def _ensure_status():
                while True:
                    try:
                        return await self.backend.update(uid, status)
                    except Exception as exc:
                        reporting.exception(exc)
                    log.error('Backend not available, retrying update in 5')
                    await asyncio.sleep(5)
            asyncio.ensure_future(_ensure_status())

    async def retrieve(self, since=None, status=None):
        """
        Return the list of events stored since the given datetime
        """
        # Retrieve in-memory
        def check_params(item):
            since_check = True
            status_check = True

            if since:
                since_check = item['created_at'] >= since

            if status:
                if isinstance(status, list):
                    status_check = EventStatus[item['status']] in status
                else:
                    status_check = item['status'] == status.value

            return since_check and status_check

        in_backend = list()

        if self.backend:
            async def _ensure_backend():
                while True:
                    try:
                        return await self.backend.retrieve(
                            since=since, status=status
                        )
                    except Exception as exc:
                        reporting.exception(exc)
                    log.error('Backend not available, retrying retrieve in 5')
                    await asyncio.sleep(5)

            in_backend = await _ensure_backend()

        in_memory = list(filter(check_params, self._last_events.list))
        return in_backend + in_memory
