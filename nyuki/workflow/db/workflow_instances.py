import re
import asyncio
import logging
from enum import Enum
from datetime import datetime, timezone
from bson.codec_options import CodecOptions
from pymongo import DESCENDING, ASCENDING
from pymongo.errors import DuplicateKeyError


log = logging.getLogger(__name__)


class Ordering(Enum):

    title_asc = ('title', ASCENDING)
    title_desc = ('title', DESCENDING)
    start_asc = ('exec.start', ASCENDING)
    start_desc = ('exec.start', DESCENDING)
    end_asc = ('exec.end', ASCENDING)
    end_desc = ('exec.end', DESCENDING)

    @classmethod
    def keys(cls):
        return [key for key in cls.__members__.keys()]


class WorkflowInstanceCollection:

    REQUESTER_REGEX = re.compile(r'^nyuki://.*')

    def __init__(self, storage):
        self._storage = storage
        # Handle timezones in mongo collections.
        # See http://api.mongodb.com/python/current/examples/datetimes.html#reading-time
        self._instances = storage.db['workflow_instances'].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        )
        asyncio.ensure_future(self.index())

    async def index(self):
        # Workflow
        await self._instances.create_index('exec.id', unique=True)
        await self._instances.create_index('exec.state')
        await self._instances.create_index('exec.requester')
        # Search and sorting indexes
        await self._instances.create_index('title')
        await self._instances.create_index('exec.start')
        await self._instances.create_index('exec.end')

    async def get_one(self, exec_id, full=False):
        """
        Return the instance with `exec_id` from workflow history.
        """
        workflow = await self._instances.find_one(
            {'exec.id': exec_id}, {'_id': 0}
        )
        if workflow:
            workflow['tasks'] = await self._storage.task_instances.get(exec_id, full)
        return workflow

    async def get(self, root=False, full=False, offset=None, limit=None,
                  since=None, state=None, search=None, order=None):
        """
        Return all instances from history from `since` with state `state`.
        """
        query = {}
        # Prepare query
        if isinstance(since, datetime):
            query['exec.start'] = {'$gte': since}
        if isinstance(state, Enum):
            query['exec.state'] = state.value
        if root is True:
            query['exec.requester'] = {'$not': self.REQUESTER_REGEX}
        if search:
            query['title'] = {'$regex': '.*{}.*'.format(search)}

        if full is False:
            fields = {
                '_id': 0,
                'title': 1,
                'exec': 1,
                'id': 1,
                'version': 1,
                'draft': 1
            }
        else:
            fields = {'_id': 0}

        cursor = self._instances.find(query, fields)
        # Count total results regardless of limit/offset
        count = await cursor.count()

        # Sort depending on Order enum values
        if order is not None:
            cursor.sort(*order)
        else:
            # End descending by default
            cursor.sort(*Ordering.end_desc.value)

        # Set offset and limit
        if isinstance(offset, int) and offset >= 0:
            cursor.skip(offset)
        if isinstance(limit, int) and limit > 0:
            cursor.limit(limit)

        # Execute query
        workflows = await cursor.to_list(None)
        if full is True:
            for workflow in workflows:
                workflow['tasks'] = await self._storage.task_instances.get(
                    workflow['exec']['id'], True
                )
        return count, workflows

    async def insert(self, workflow):
        """
        Insert a finished workflow report into the workflow history.
        """
        # Split tasks exec and workflow exec.
        for task in workflow['tasks']:
            task['workflow_exec_id'] = workflow['exec']['id']
            await self._storage.task_instances.insert(task)
        del workflow['tasks']

        try:
            await self._instances.insert(workflow)
        except DuplicateKeyError:
            # If it's a duplicate, we don't want to lose it
            workflow['exec']['duplicate'] = workflow['exec']['id']
            workflow['exec']['id'] = str(uuid4())
            await self._instances.insert(workflow)
