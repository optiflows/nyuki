import re
import asyncio
import logging
from enum import Enum
from uuid import uuid4
from datetime import datetime, timezone
from bson.codec_options import CodecOptions
from pymongo import DESCENDING, ASCENDING
from pymongo.errors import DuplicateKeyError


log = logging.getLogger(__name__)


class Ordering(Enum):

    title_asc = ('template.title', ASCENDING)
    title_desc = ('template.title', DESCENDING)
    start_asc = ('start', ASCENDING)
    start_desc = ('start', DESCENDING)
    end_asc = ('end', ASCENDING)
    end_desc = ('end', DESCENDING)

    @classmethod
    def keys(cls):
        return [key for key in cls.__members__.keys()]


class WorkflowInstancesCollection:

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
        await self._instances.create_index('id', unique=True)
        await self._instances.create_index('state')
        await self._instances.create_index('requester')
        # Search and sorting indexes
        await self._instances.create_index('template.title')
        await self._instances.create_index('start')
        await self._instances.create_index('end')

    async def get_one(self, exec_id, full=False):
        """
        Return the instance with `exec_id` from workflow history.
        """
        workflow = await self._instances.find_one(
            {'id': exec_id}, {'_id': 0}
        )
        if workflow:
            workflow['template']['tasks'] = await self._storage.task_instances.get(exec_id, full)
        return workflow

    async def get(self, root=False, full=False, offset=None, limit=None,
                  since=None, state=None, search=None, order=None):
        """
        Return all instances from history from `since` with state `state`.
        """
        query = {}
        # Prepare query
        if isinstance(since, datetime):
            query['start'] = {'$gte': since}
        if isinstance(state, Enum):
            query['state'] = state.value
        if root is True:
            query['requester'] = {'$not': self.REQUESTER_REGEX}
        if search:
            query['template.title'] = {'$regex': '.*{}.*'.format(search)}

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
                workflow['template']['tasks'] = await self._storage.task_instances.get(
                    workflow['id'], True
                )
        return count, workflows

    async def insert(self, workflow):
        """
        Insert a finished workflow report into the workflow history.
        """
        try:
            await self._instances.insert(workflow)
        except DuplicateKeyError:
            # If it's a duplicate, we don't want to lose it
            workflow['duplicate'] = workflow['id']
            workflow['id'] = str(uuid4())
            await self._instances.insert(workflow)
