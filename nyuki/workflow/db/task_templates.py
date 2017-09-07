import asyncio
import logging
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError


log = logging.getLogger(__name__)


class TaskTemplatesCollection:

    """
    {
        "id": <str>,
        "name": <str>,
        "config": {},
        "topics": [<str>],
        "title": <str>,
        "workflow_template": {
            "id": <uuid4>,
            "version": <int>
        }
    }
    """

    def __init__(self, storage):
        self._templates = storage.db['task_templates']
        asyncio.ensure_future(self.index())

    async def index(self):
        # Pair of indexes on the workflow template id/version
        await self._templates.create_index([
            ('id', ASCENDING),
            ('workflow_template.id', ASCENDING),
            ('workflow_template.version', DESCENDING),
        ], unique=True)

    async def get(self, workflow_id, version):
        """
        Return all the task template of a workflow and a version.
        """
        cursor = self._templates.find(
            {
                'workflow_template.id': workflow_id,
                'workflow_template.version': version
            },
            {
                '_id': 0,
                'workflow_template': 0,
            },
        )
        return await cursor.to_list(None)

    async def insert(self, task):
        """
        Insert a whole task depending on its id/version.
        """
        query = {
            'id': task['id'],
            'workflow_template.id': task['workflow_template']['id'],
            'workflow_template.version': task['workflow_template']['version'],
        }
        await self._templates.replace_one(query, task, upsert=True)

    async def delete(self, tid):
        await self._templates.delete_many({'id': tid})
