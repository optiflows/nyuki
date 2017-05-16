import asyncio
import logging

from nyuki.utils import utcnow
from nyuki.bus import reporting
from nyuki.bus.persistence.backend import PersistenceBackend
from nyuki.bus.persistence.events import EventStatus
from nyuki.bus.persistence.mongo_backend import MongoBackend
from nyuki.bus.persistence.memory_backend import MemoryBackend


log = logging.getLogger(__name__)


class PersistenceError(Exception):
    pass


class BusPersistence:

    """
    This module will enable local caching for bus events to replace the
    current asyncio cache which is out of our control. (cf internal NYUKI-59)
    """

    FEED_DELAY = 5

    def __init__(self, backend=None, **kwargs):
        self._loop = asyncio.get_event_loop()

        if backend == 'mongo':
            self.backend = MongoBackend(**kwargs)
        elif backend == 'memory':
            self.backend = MemoryBackend(**kwargs)
        else:
            raise PersistenceError

    async def init(self):
        """
        Init backend
        """
        return await self.backend.init()

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
        await self.backend.store(event)

    async def update(self, uid, status):
        """
        Update the status of a stored event
        """
        log.debug("Updating status of event '%s' to '%s'", uid, status)
        await self.backend.update(uid, status)

    async def retrieve(self, since=None, status=None):
        """
        Return the list of events stored since the given datetime
        """
        log.debug('Retrieving events since %s, with status %s', since, status)
        return await self.backend.retrieve(since, status)
