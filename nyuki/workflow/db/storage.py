import logging
from copy import deepcopy

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ServerSelectionTimeoutError

from .triggers import TriggerCollection
from .data_processing import DataProcessingCollection
from .metadata import MetadataCollection
from .workflow_templates import WorkflowTemplatesCollection, TemplateState
from .task_templates import TaskTemplatesCollection
from .workflow_instances import WorkflowInstancesCollection
from .task_instances import TaskInstancesCollection


log = logging.getLogger(__name__)


class MongoStorage:

    def __init__(self):
        self._client = None
        self._db = None
        self._validate_on_start = False

        # Collections
        self._workflow_templates = None
        self._task_templates = None
        self._workflow_metadata = None
        self._workflow_instances = None
        self._task_instances = None
        self.regexes = None
        self.lookups = None
        self.triggers = None

    def configure(self, host, database, validate_on_start=True, **kwargs):
        log.info(
            "Setting up mongo storage with host '%s' and database '%s'",
            host, database,
        )
        self._client = AsyncIOMotorClient(host, **kwargs)
        self._db = self._client[database]
        self._validate_on_start = validate_on_start

        # Collections
        self._workflow_templates = WorkflowTemplatesCollection(self._db)
        self._task_templates = TaskTemplatesCollection(self._db)
        self._workflow_metadata = MetadataCollection(self._db)
        self._workflow_instances = WorkflowInstancesCollection(self._db)
        self._task_instances = TaskInstancesCollection(self._db)
        self.regexes = DataProcessingCollection(self._db, 'regexes')
        self.lookups = DataProcessingCollection(self._db, 'lookups')
        self.triggers = TriggerCollection(self._db)

    async def index(self):
        """
        Try to connect to mongo and index all the collections.
        """
        log.info('Trying to connect to Mongo...')
        while True:
            try:
                await self._workflow_templates.index()
                await self._task_templates.index()
                await self._workflow_metadata.index()
                await self._workflow_instances.index()
                await self._task_instances.index()
                await self.regexes.index()
                await self.lookups.index()
                await self.triggers.index()
            except ServerSelectionTimeoutError as exc:
                log.error('Could not connect to Mongo - %s', exc)
            else:
                log.info('Successfully connected to Mongo')
                break

        if self._validate_on_start is True:
            collections = await self._db.collection_names()
            log.info('Validating %s collections', len(collections))
            for collection in collections:
                await self._db.validate_collection(collection)
                log.info('Validated collection %s', collection)

    # Templates

    async def update_workflow_metadata(self, tid, metadata):
        """
        Update and return
        """
        return await self._workflow_metadata.update(tid, metadata)

    async def upsert_draft(self, template):
        """
        Update a template's draft and all its associated tasks.
        """
        metadata = await self._workflow_metadata.get_one(template['id'])
        if not metadata:
            metadata = {
                'workflow_template_id': template['id'],
                'title': template.pop('title'),
                'tags': template.pop('tags', []),
            }
            await self._workflow_metadata.insert(metadata)

        # Force set of values 'version' and 'draft'.
        template['version'] = await self._workflow_templates.get_last_version(
            template['id']
        ) + 1
        template['state'] = TemplateState.DRAFT.value

        # Split and insert tasks.
        tasks = template.pop('tasks')
        await self._task_templates.insert_many(deepcopy(tasks), template)

        # Insert template without tasks.
        await self._workflow_templates.insert_draft(template)
        template['tasks'] = tasks
        template.update({'title': metadata['title'], 'tags': metadata['tags']})
        return template

    async def publish_draft(self, template_id):
        """
        Publish a draft into an 'active' state, and archive the old active.
        """
        await self._workflow_templates.publish_draft(template_id)
        log.info('Draft for template %s published', template_id[:8])

    async def get_for_topic(self, topic):
        """
        Return all the templates listening on a particular topic.
        This does not append the metadata.
        """
        templates = await self._workflow_templates.get_for_topic(topic)
        if templates:
            log.info(
                'Fetched %s templates for event from "%s"',
                len(templates), topic,
            )
        for template in templates:
            template['tasks'] = await self._task_templates.get(
                template['id'], template['version']
            )
        return templates

    async def get_templates(self, template_id=None, full=False):
        """
        Return all active/draft templates
        Limited to a small set of fields if 'full' is False.
        TODO: Pagination.
        """
        templates = await self._workflow_templates.get(template_id, full)
        for template in templates:
            metadata = await self._workflow_metadata.get_one(template['id'])
            template.update(metadata)
            if full is True:
                template['tasks'] = await self._task_templates.get(
                    template['id'], template['version']
                )
        return templates

    async def get_template(self, tid, draft=False, version=None):
        """
        Return the active template.
        """
        template = await self._workflow_templates.get_one(
            tid,
            draft=draft,
            version=int(version) if version else None,
        )
        if not template:
            return

        metadata = await self._workflow_metadata.get_one(tid)
        template.update(metadata)
        template['tasks'] = await self._task_templates.get(
            template['id'], template['version']
        )
        return template

    async def delete_template(self, tid, draft=False):
        """
        Delete a whole template or only its draft.
        """
        await self._workflow_templates.delete(tid, draft)
        if draft is False:
            await self._task_templates.delete_many(tid)
            await self._workflow_metadata.delete(tid)
            await self.triggers.delete(tid)

    # Instances

    async def insert_instance(self, instance):
        """
        Insert a static workflow instance and all its tasks.
        """
        task_instances = []
        for task in instance['template'].pop('tasks'):
            task['workflow_instance_id'] = instance['id']
            task_instances.append(task)
        await self._task_instances.insert_many(task_instances)
        await self._workflow_instances.insert(instance)

    # History

    async def get_history(self, **kwargs):
        """
        Return paginated workflow history.
        """
        count, workflows = await self._workflow_instances.get(**kwargs)
        if kwargs.get('full') is True:
            for workflow in workflows:
                workflow['template']['tasks'] = await self._task_instances.get(
                    workflow['id'], True
                )
        return count, workflows

    async def get_instance(self, instance_id, full=False):
        workflow = await self._workflow_instances.get_one(instance_id, full)
        if not workflow:
            return
        workflow['template']['tasks'] = await self._task_instances.get(
            workflow['id'], full
        )
        return workflow

    async def get_instance_task(self, task_id, full=False):
        return await self._task_instances.get_one(task_id, full)

    async def get_instance_task_data(self, task_id):
        return await self._task_instances.get_data(task_id)
