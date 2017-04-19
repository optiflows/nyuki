import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient

from .api.workflows import InstanceCollections
from .api.templates import TemplateCollection


log = logging.getLogger(__name__)


class _DataProcessingCollection:

    def __init__(self, data_collection):
        self._rules = data_collection
        asyncio.ensure_future(self._rules.create_index('id', unique=True))

    async def get_all(self):
        """
        Return a list of all rules
        """
        cursor = self._rules.find(None, {'_id': 0})
        return await cursor.to_list(None)

    async def get(self, rule_id):
        """
        Return the rule for given id or None
        """
        cursor = self._rules.find({'id': rule_id}, {'_id': 0})
        await cursor.fetch_next
        return cursor.next_object()

    async def insert(self, data):
        """
        Insert a new data processing rule:
        {
            "id": "rule_id",
            "name": "rule_name",
            "config": {
                "some": "configuration"
            }
        }
        """
        query = {'id': data['id']}
        log.info(
            "Inserting data processing rule in collection '%s'",
            self._rules.name
        )
        log.debug('upserting data: %s', data)
        await self._rules.update(query, data, upsert=True)

    async def delete(self, rule_id=None):
        """
        Delete a rule from its id or all rules
        """
        query = {'id': rule_id} if rule_id is not None else None
        log.info("Removing rule(s) from collection '%s'", self._rules.name)
        log.debug('delete query: %s', query)
        await self._rules.remove(query)


class _TriggerCollection:

    def __init__(self, data_collection):
        self._triggers = data_collection
        asyncio.ensure_future(self._triggers.create_index('tid', unique=True))

    async def get_all(self):
        """
        Return a list of all trigger forms
        """
        cursor = self._triggers.find(None, {'_id': 0})
        return await cursor.to_list(None)

    async def get(self, template_id):
        """
        Return the trigger form of a given workflow template id
        """
        cursor = self._triggers.find({'tid': template_id}, {'_id': 0})
        await cursor.fetch_next
        return cursor.next_object()

    async def insert(self, tid, form):
        """
        Insert a trigger form for the given workflow template
        """
        data = {'tid': tid, 'form': form}
        await self._triggers.update({'tid': tid}, data, upsert=True)
        return data

    async def delete(self, template_id=None):
        """
        Delete a trigger form
        """
        query = {'tid': template_id} if template_id is not None else None
        await self._triggers.remove(query)


class MongoStorage:

    DEFAULT_DATABASE = 'workflow'

    def __init__(self, host, database=None, **kwargs):
        log.info("Setting up workflow mongo storage with host '%s'", host)
        client = AsyncIOMotorClient(host, **kwargs)
        db_name = database or self.DEFAULT_DATABASE
        db = client[db_name]
        log.info("Workflow database: '%s'", db_name)

        # Collections
        self.templates = TemplateCollection(db['templates'], db['metadata'])
        self.instances = InstanceCollections(db)
        self.regexes = _DataProcessingCollection(db['regexes'])
        self.lookups = _DataProcessingCollection(db['lookups'])
        self.triggers = _TriggerCollection(db['triggers'])
