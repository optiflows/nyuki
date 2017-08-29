import asyncio
import logging
from datetime import timezone
from bson.codec_options import CodecOptions


log = logging.getLogger(__name__)
WS_FILTERS = ('quorum', 'status', 'twilio_error', 'timeout', 'diff')


class TaskInstanceCollection:

    TASK_HISTORY_FILTERS = {
        '_id': 0,
        # Tasks
        'id': 1,
        'name': 1,
        'config': 1,
        'topics': 1,
        'title': 1,
        # Exec
        'exec.id': 1,
        'exec.start': 1,
        'exec.end': 1,
        'exec.state': 1,
        # Graph-specific data fields
        **{'exec.outputs.{}'.format(key): 1 for key in WS_FILTERS}
    }

    def __init__(self, storage):
        # Handle timezones in mongo collections.
        # See http://api.mongodb.com/python/current/examples/datetimes.html#reading-time
        self._instances = storage.db['task_instances'].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        )
        asyncio.ensure_future(self.index())

    async def index(self):
        await self._instances.create_index('workflow_exec_id')
        await self._instances.create_index('exec.id')

    async def get(self, wid, full=False):
        """
        Return all task instances of one workflow.
        """
        if full is False:
            filters = self.TASK_HISTORY_FILTERS
        else:
            filters = {'_id': 0, 'workflow_exec_id': 0}
        cursor = self._instances.find({'workflow_exec_id': wid}, filters)
        return await cursor.to_list(None)

    async def get_one(self, tid, full=False):
        """
        Return one task instance.
        """
        if full is False:
            filters = {'_id': 0, 'exec.inputs': 0, 'exec.outputs': 0}
        else:
            filters = {'_id': 0}
        return await self._instances.find_one({'exec.id': tid}, filters)

    async def get_data(self, tid):
        """
        Return the data (inputs/outputs) of one executed task.
        """
        task = await self._instances.find_one(
            {'exec.id': tid},
            {'_id': 0, 'exec.inputs': 1, 'exec.outputs': 1},
        )
        return {
            'inputs': task['exec']['inputs'],
            'outputs': task['exec']['outputs'],
        } if task else None

    async def insert(self, task):
        """
        Insert a finished workflow task.
        """
        await self._instances.insert(task)
