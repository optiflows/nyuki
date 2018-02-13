import logging
from datetime import timezone
from bson.codec_options import CodecOptions


log = logging.getLogger(__name__)
WS_FILTERS = ('quorum', 'status', 'twilio_error', 'diff')


class TaskInstancesCollection:

    TASK_HISTORY_FILTERS = {
        '_id': 0,
        # Tasks
        'template.id': 1,
        'template.name': 1,
        'template.config': 1,
        'template.topics': 1,
        'template.title': 1,
        'template.timeout': 1,
        # Exec
        'id': 1,
        'start': 1,
        'end': 1,
        'state': 1,
        # Graph-specific data fields
        **{'outputs.{}'.format(key): 1 for key in WS_FILTERS}
    }

    def __init__(self, db):
        # Handle timezones in mongo collections.
        # See http://api.mongodb.com/python/current/examples/datetimes.html#reading-time
        self._instances = db['task_instances'].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=timezone.utc)
        )

    async def index(self):
        await self._instances.create_index('id', unique=True)
        await self._instances.create_index('workflow_instance_id')

    async def get(self, wid, full=False):
        """
        Return all task instances of one workflow.
        """
        if full is False:
            filters = self.TASK_HISTORY_FILTERS
        else:
            filters = {'_id': 0, 'workflow_instance_id': 0}
        cursor = self._instances.find({'workflow_instance_id': wid}, filters)
        return await cursor.to_list(None)

    async def get_one(self, tid, full=False):
        """
        Return one task instance.
        """
        filters = {'_id': 0, 'workflow_instance_id': 0}
        if full is False:
            filters.update({'inputs': 0, 'outputs': 0})
        return await self._instances.find_one({'id': tid}, filters)

    def get_for_csv(self, workflow_id):
        """
        Return the data needed for a CSV import of one workflow execution.
        To be used with `async for task in get_for_csv():`
        """
        return self._instances.find({
            'workflow_instance_id': workflow_id,
            'template.name': {'$in': ['send_sms', 'call', 'send_email', 'wait_sms', 'wait_call', 'wait_email']},
            'reporting': {'$ne': None},
        }, {
            'start': 1,
            'template.id': 1,
            'template.title': 1,
            'template.name': 1,
            'reporting.contacts': 1,
        })

    async def get_data(self, tid):
        """
        Return the data (inputs/outputs) of one executed task.
        """
        return await self._instances.find_one(
            {'id': tid},
            {'_id': 0, 'inputs': 1, 'outputs': 1},
        )

    async def insert_many(self, tasks):
        """
        Insert all the tasks of a finished workflow.
        """
        await self._instances.insert_many(tasks)
