import asyncio
import logging

from .utils.indexes import check_index_names


log = logging.getLogger(__name__)


class TriggerCollection:

    def __init__(self, db):
        self._triggers = db['triggers']

    async def index(self):
        await check_index_names(self._triggers, ['unique_tid'])
        await self._triggers.create_index('tid', unique=True, name='unique_tid')

    async def get(self):
        """
        Return a list of all trigger forms
        """
        cursor = self._triggers.find(None, {'_id': 0})
        return await cursor.to_list(None)

    async def get_one(self, template_id):
        """
        Return the trigger form of a given workflow template id
        """
        return await self._triggers.find_one({'tid': template_id}, {'_id': 0})

    async def insert(self, tid, form):
        """
        Insert a trigger form for the given workflow template
        """
        data = {'tid': tid, 'form': form}
        await self._triggers.replace_one({'tid': tid}, data, upsert=True)
        return data

    async def delete(self, tid):
        """
        Delete a trigger form
        """
        await self._triggers.delete_one({'tid': tid})
