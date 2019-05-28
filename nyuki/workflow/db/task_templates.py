import logging

from pymongo import ASCENDING, DESCENDING, ReplaceOne
from pymongo.errors import BulkWriteError

from .utils.indexes import check_index_names


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

    def __init__(self, db):
        self._templates = db['task_templates']

    async def index(self):
        await check_index_names(
            self._templates, ['unique_uid_workflow_template_id_version'],
        )
        # Pair of indexes on the workflow template id/version
        await self._templates.create_index(
            [
                ('id', ASCENDING),
                ('workflow_template.id', ASCENDING),
                ('workflow_template.version', DESCENDING),
            ],
            unique=True,
            name='unique_uid_workflow_template_id_version'
        )

    async def get(self, workflow_id, version):
        """
        Return all the task template of a workflow and a version.
        """
        cursor = self._templates.find(
            {
                'workflow_template.id': workflow_id,
                'workflow_template.version': version,
            },
            {
                '_id': 0,
                'workflow_template': 0,
            },
        )
        return await cursor.to_list(None)

    async def insert_many(self, tasks, template):
        """
        Update multiple tasks at once, remove the now unused tasks.
        """
        # Delete all tasks that are not in the list (draft mode).
        await self._templates.delete_many({
            'id': {'$nin': [task['id'] for task in tasks]},
            'workflow_template.id': template['id'],
            'workflow_template.version': template['version'],
        })

        # Re-update all the tasks.
        ops = []
        for task in tasks:
            task['workflow_template'] = {
                'id': template['id'],
                'version': template['version'],
            }
            ops.append(ReplaceOne(
                {
                    'id': task['id'],
                    'workflow_template.id': template['id'],
                    'workflow_template.version': template['version']
                },
                task, upsert=True
            ))

        if ops:
            try:
                await self._templates.bulk_write(ops, ordered=False)
            except BulkWriteError as exc:
                log.error(
                    'Errors while updating tasks in workflow template %s: %s',
                    template['id'], exc.details
                )

    async def delete_many(self, tid):
        """
        Delete all tasks of one template.
        """
        await self._templates.delete_many({'workflow_template.id': tid})
