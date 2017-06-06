import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import (
    AutoReconnect, OperationFailure, ServerSelectionTimeoutError
)

from nyuki.bus.persistence.backend import PersistenceBackend


log = logging.getLogger(__name__)


class MongoNotConnectedError(Exception):
    pass


class MongoBackend(PersistenceBackend):

    def __init__(self, name, host='localhost', ttl=3600, **kwargs):
        self.name = name
        self.client = None
        self.db = None
        self.host = host
        self.ttl = ttl
        self._collection = None
        # Options
        self._options = kwargs

    def __repr__(self):
        return "<MongoBackend host='{}' col='{}'>".format(self.host, self.name)

    async def init(self):
        # Get collection for this nyuki
        self.client = AsyncIOMotorClient(self.host, **self._options)
        self.db = self.client['bus_persistence']
        self._collection = self.db[self.name]
        await self._index_ttl()

    async def _index_ttl(self):
        # Set a TTL to the documents in this collection
        async def index():
            await self._collection.create_index('id')
            await self._collection.create_index(
                'created_at', expireAfterSeconds=self.ttl
            )

        log.info('Indexation of mongo fields')
        try:
            await index()
        except OperationFailure:
            # Value changed, drop and reindex
            await self._collection.drop_index('created_at_1')
            await index()
        except ServerSelectionTimeoutError:
            log.error('Could not index mongo fields')
            raise

    async def store(self, event):
        try:
            await self._collection.insert(event)
        except AutoReconnect:
            log.error('Backend not available: %r', self)

    async def update(self, uid, status):
        try:
            await self._collection.update(
                {'id': uid},
                {'$set': {'status': status.value}}
            )
        except AutoReconnect:
            log.error('Backend not available: %r', self)

    async def retrieve(self, since=None, status=None):
        query = {}
        if since:
            query['created_at'] = {'$gte': since}
        if status:
            if isinstance(status, list):
                query['status'] = {'$in': [es.value for es in status]}
            else:
                query['status'] = status.value

        cursor = self._collection.find(query)
        cursor.sort('created_at')

        try:
            return await cursor.to_list(None)
        except AutoReconnect:
            log.error('Backend not available: %r', self)
