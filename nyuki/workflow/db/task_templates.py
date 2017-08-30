import asyncio
import logging
from pymongo import DESCENDING
from pymongo.errors import DuplicateKeyError


log = logging.getLogger(__name__)


class TaskTemplateCollection:

    def __init__(self, storage):
        self._templates = storage.db['task_templates']
        asyncio.ensure_future(self.index())

    async def index(self):
        await self._templates.create_index(
            [('id', DESCENDING), ('version', DESCENDING)],
            unique=True
        )

    async def get(self, tids, version):
        """
        Return all the task template with a list of ids and a version.
        """
        cursor = self._templates.find(
            {'id': {'$in': tids}, 'version': version},
            {'_id': 0},
        )
        return await cursor.to_list(None)

    async def insert(self, task):
        """
        Insert a whole task depending on its id/version.
        """
        await self._templates.replace_one(
            {'id': task['id'], 'version': task['version']},
            task,
            upsert=True,
        )

    async def delete(self, tid):
        await self._templates.delete_many({'id': tid})
