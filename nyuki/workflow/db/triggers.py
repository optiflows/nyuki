import asyncio
import logging


log = logging.getLogger(__name__)


class TriggerCollection:

    def __init__(self, storage):
        self._triggers = storage.db['triggers']
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
        await self._triggers.delete_one(query)
