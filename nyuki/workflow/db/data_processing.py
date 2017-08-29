import asyncio
import logging


log = logging.getLogger(__name__)


class DataProcessingCollection:

    def __init__(self, storage, collection_name):
        self._rules = storage.db[collection_name]
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