import time
import asyncio
import logging
from uuid import uuid4
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
from motor.motor_asyncio import AsyncIOMotorClient


log = logging.getLogger(__name__)


def timed(func):
    async def wraps(*args, **kwargs):
        start = time.time()
        r = await func(*args, **kwargs)
        log.debug("'%s' took %s s", func.__name__, time.time() - start)
        return r
    return wraps


class Migrator:

    """
    Used to fill a collection and progressively erase an old one.
    """

    def __init__(self, old_collection, new_collection):
        self._old_collection = old_collection
        self._new_collection = new_collection
        self._old_bulk = old_collection.initialize_unordered_bulk_op()
        self._new_bulk = new_collection.initialize_unordered_bulk_op()
        self._cursor = old_collection.find()
        self._batch_size = 0
        self.count = 0

    @property
    def old(self):
        return self._old_bulk

    @property
    def new(self):
        return self._new_bulk

    def __aiter__(self):
        return self

    async def __anext__(self):
        self._batch_size += 1
        self.count += 1

        # Insert in bulk. The batch size is a cpu/memory compromise.
        if self._batch_size == 200:
            await asyncio.wait([
                asyncio.ensure_future(self._old_bulk.execute()),
                asyncio.ensure_future(self._new_bulk.execute()),
            ])
            self._batch_size = 0
            self._old_bulk = self._old_collection.initialize_unordered_bulk_op()
            self._new_bulk = self._new_collection.initialize_unordered_bulk_op()

        # Return one document.
        await self._cursor.fetch_next
        doc = self._cursor.next_object()

        # End of iteration, insert the rest.
        if not doc:
            if self._batch_size > 0:
                await asyncio.wait([
                    asyncio.ensure_future(self._old_bulk.execute()),
                    asyncio.ensure_future(self._new_bulk.execute()),
                ])
            await self._old_collection.drop()
            raise StopAsyncIteration

        return doc


class Migration:

    def __init__(self, host, database, **kwargs):
        client = AsyncIOMotorClient(host, **kwargs)
        self.db = client[database]

    @timed
    async def run(self):
        """
        Run the migrations.
        """
        log.info("Starting migrations on database '%s'", self.db.name)
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
        log.info("Migration on database '%s' passed", self.db.name)

    @timed
    async def _migrate_workflow_metadata(self):
        old_col = self.db['metadata']
        new_col = self.db['workflow_metadata']
        migrator = Migrator(old_col, new_col)
        async for metadata in migrator:
            metadata['workflow_template_id'] = metadata.pop('id')
            migrator.new.insert(metadata)
            migrator.old.find({'_id': metadata['_id']}).remove_one()
        log.info('%s workflow metadata migrated', migrator.count)

    @timed
    async def _migrate_workflow_templates(self):
        """
        Replace documents with new structure.
        This uses a particular search filter because of the 'draft' attribute.
        """
        count = 0
        template_state = [None, None]
        sort = [('id', ASCENDING), ('version', DESCENDING)]
        wf_col = self.db['workflow_templates']
        task_col = self.db['task_templates']

        # Sorted on version number to set the proper 'state' values.
        async for template in self.db['templates'].find(None, sort=sort):
            count += 1
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
            if tasks:
                await task_bulk.execute()

        await self.db['templates'].drop()
        log.info('%s workflow templates splited and migrated', count)

    @timed
    async def _migrate_workflow_instances(self):
        """
        Replace workflow instance documents with new structure.
        """
        old_col = self.db['workflow-instances']
        new_col = self.db['workflow_instances']
        migrator = Migrator(old_col, new_col)
        async for workflow in migrator:
            # Convert workflow instance.
            instance = workflow.pop('exec')
            instance['_id'] = workflow.pop('_id')
            instance['template'] = workflow
            # Insert.
            migrator.new.insert(instance)
            migrator.old.find({'_id': instance['_id']}).remove_one()
        log.info('%s workflow instances migrated', migrator.count)

    def _migrate_one_task_template(self, template):
        """
        Do the task configuration migrations.
        Add the new 'timeout' field.
        """
        config = template['config']
        template['timeout'] = None

        if template['name'] == 'join':
            template['timeout'] = config.pop('timeout', None)

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
        old_col = self.db['task-instances']
        new_col = self.db['task_instances']
        migrator = Migrator(old_col, new_col)
        async for task in migrator:
            # Convert task instance.
            instance = self._new_task(task)
            # Insert.
            migrator.new.insert(instance)
            migrator.old.find({'_id': instance['_id']}).remove_one()
        log.info('%s task instances migrated', migrator.count)

    @timed
    async def _migrate_old_instances(self):
        """
        Bring back the old 'instances' collection from the dead.
        """
        task_count = 0
        old_col = self.db['instances']
        workflow_col = self.db['workflow_instances']
        task_col = self.db['task_instances']
        migrator = Migrator(old_col, workflow_col)
        async for workflow in migrator:
            # Split tasks from their workflow instance.
            tasks = workflow.pop('tasks')
            # Convert workflow instance.
            instance = workflow.pop('exec')
            instance['_id'] = workflow.pop('_id')
            instance['template'] = workflow
            task_count += len(tasks)
            # Insert workflow instance.
            migrator.new.insert(instance)
            migrator.old.find({'_id': instance['_id']}).remove_one()

            try:
                # Convert and insert task instances.
                await task_col.insert_many([
                    self._new_task(task, instance['id'])
                    for task in tasks
                ])
            except BulkWriteError:
                pass

        log.info(
            '%s old instances migrated to new format (including %s tasks)',
            migrator.count, task_count,
        )


if __name__ == '__main__':
    logging.basicConfig(format='%(message)s', level='DEBUG')
    m = Migration('localhost', 'twilio')
    asyncio.get_event_loop().run_until_complete(m.run())
