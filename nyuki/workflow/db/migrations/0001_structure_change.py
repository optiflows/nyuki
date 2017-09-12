import time
import asyncio
import logging
from uuid import uuid4
from bson.objectid import ObjectId
from pymongo import ASCENDING, DESCENDING
from motor.motor_asyncio import AsyncIOMotorClient


log = logging.getLogger(__name__)


def timed(func):
    async def wraps(*args, **kwargs):
        start = time.time()
        r = await func(*args, **kwargs)
        log.debug("'%s' took %s s", func.__name__, time.time() - start)
        return r
    return wraps


class Migration:

    def __init__(self, host, database, **kwargs):
        client = AsyncIOMotorClient(host, **kwargs)
        self.db = client[database]

    @timed
    async def run(self):
        """
        Run the migrations.
        """
        log.info('Migration started')
        collections = await self.db.collection_names()
        if 'metadata' in collections:
            await self._migrate_workflow_metadata()
        if 'templates' in collections:
            await self._migrate_workflow_templates()
        if 'workflow-instances' in collections:
            await self._migrate_workflow_instances()
        if 'task-instances' in collections:
            await self._migrate_task_instances()
        if 'instances' in collections:
            await self._migrate_old_instances()
        log.info('Migration passed')

    @timed
    async def _migrate_workflow_metadata(self):
        col = self.db['workflow_metadata']
        async for metadata in self.db['metadata'].find():
            metadata['workflow_template_id'] = metadata.pop('id')
            await col.replace_one({'_id': metadata['_id']}, metadata, upsert=True)
        await self.db['metadata'].drop()
        log.info('Workflow metadata migrated')

    @timed
    async def _migrate_workflow_templates(self):
        """
        Replace documents with new structure.
        """
        template_state = [None, None]
        sort = [('id', ASCENDING), ('version', DESCENDING)]
        wf_col = self.db['workflow_templates']
        task_col = self.db['task_templates']

        # Sorted on version number to set the proper 'state' values.
        async for template in self.db['templates'].find(None, sort=sort):
            # Ensure we got only one 'draft' if any, one 'active' if any,
            # and the rest 'archived'.
            if template['id'] != template_state[0]:
                template_state = [template['id'], 'active']
            if template['draft'] is True:
                state = 'draft'
            else:
                state = template_state[1]
                if state == 'active':
                    template_state[1] = 'archived'

            template['state'] = state
            template['timeout'] = None
            del template['draft']
            # Remove tasks.
            tasks = template.pop('tasks')
            await wf_col.replace_one(
                {'_id': template['_id']}, template, upsert=True
            )

            # Migrate and insert task templates.
            workflow_template = {
                'id': template['id'],
                'version': template['version'],
            }
            task_bulk = task_col.initialize_unordered_bulk_op()
            for task in tasks:
                task['workflow_template'] = workflow_template
                task = self._migrate_one_task_template(task)
                task_bulk.find({
                    'id': task['id'],
                    'workflow_template.id': workflow_template['id'],
                    'workflow_template.version': workflow_template['version'],
                }).upsert().replace_one(task)
            await task_bulk.execute()

        await self.db['templates'].drop()
        log.info('Workflow templates splited and migrated')

    @timed
    async def _migrate_workflow_instances(self):
        """
        Replace workflow instance documents with new structure.
        """
        i = 0
        col = self.db['workflow_instances']
        bulk = col.initialize_unordered_bulk_op()
        async for workflow in self.db['workflow-instances'].find():
            i += 1
            instance = workflow.pop('exec')
            instance['_id'] = workflow.pop('_id')
            instance['template'] = workflow
            bulk.find({'_id': instance['_id']}).upsert().replace_one(instance)

            if i == 1000:
                await bulk.execute()
                bulk = col.initialize_unordered_bulk_op()
                i = 0

        if i > 0:
            await bulk.execute()

        await self.db['workflow-instances'].drop()
        log.info('Workflow instances migrated')

    def _migrate_one_task_template(self, template):
        """
        Do the task configuration migrations.
        Add the new 'timeout' field.
        """
        config = template['config']

        if template['name'] == 'join':
            template['timeout'] = config.get('timeout')

        elif template['name'] == 'trigger_workflow':
            template['timeout'] = config.get('timeout')
            template['config'] = {
                'blocking': config.get('await_completion', True),
                'template': {
                    'service': 'twilio' if 'twilio' in config['nyuki_api'] else 'pipeline',
                    'id': config['template'],
                    'draft': config.get('draft', False),
                },
            }

        elif template['name'] in ['call', 'wait_sms', 'wait_email', 'wait_call']:
            if 'blocking' in config:
                template['timeout'] = config['blocking']['timeout']
                template['config']['blocking'] = True

        return template

    def _new_task(self, task, workflow_instance_id=None):
        """
        Migrate a task instance.
        """
        # If task was never executed, fill it with 'not-started'.
        instance = task.pop('exec') or {
            'id': str(uuid4()),
            'status': 'not-started',
            'start': None,
            'end': None,
            'inputs': None,
            'outputs': None,
            'reporting': None,
        }
        if '_id' in task:
            instance['_id'] = task.pop('_id')
        instance['workflow_instance_id'] = workflow_instance_id or task.pop('workflow_exec_id')
        instance['template'] = self._migrate_one_task_template(task)
        return instance

    @timed
    async def _migrate_task_instances(self):
        """
        Replace documents with new structure.
        """
        i = 0
        col = self.db['task_instances']
        bulk = col.initialize_unordered_bulk_op()
        async for task in self.db['task-instances'].find():
            i += 1
            instance = self._new_task(task)
            bulk.find({'_id': instance['_id']}).upsert().replace_one(instance)

            if i == 1000:
                await bulk.execute()
                bulk = col.initialize_unordered_bulk_op()
                i = 0

        if i > 0:
            await bulk.execute()

        await self.db['task-instances'].drop()
        log.info('Task instances migrated')

    @timed
    async def _migrate_old_instances(self):
        """
        Bring back the old 'instances' collection from the dead.
        """
        i = 0
        workflows_col = self.db['workflow_instances']
        tasks_col = self.db['task_instances']
        bulk = workflows_col.initialize_unordered_bulk_op()
        async for workflow in self.db['instances'].find():
            i += 1

            tasks = workflow.pop('tasks')
            instance = workflow.pop('exec')
            instance['_id'] = workflow.pop('_id')
            instance['template'] = workflow
            bulk.find({'_id': instance['_id']}).upsert().replace_one(instance)

            task_bulk = tasks_col.initialize_unordered_bulk_op()
            for task in tasks:
                task = self._new_task(task, instance['_id'])
                # This is an older collection, so we don't use '_id'.
                task_bulk.find({'id': task['id']}).upsert().replace_one(task)
            await task_bulk.execute()

            if i == 1000:
                await bulk.execute()
                bulk = workflows_col.initialize_unordered_bulk_op()
                i = 0

        if i > 0:
            await bulk.execute()

        await self.db['instances'].drop()
        log.info('Old instances migrated to new format')


if __name__ == '__main__':
    FORMAT = '%(asctime)-24s %(levelname)-8s [%(name)s] %(message)s'
    logging.basicConfig(format=FORMAT, level='DEBUG')
    m = Migration('localhost', 'twilio')
    asyncio.get_event_loop().run_until_complete(m.run())
