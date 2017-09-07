import logging
from motor.motor_asyncio import AsyncIOMotorClient

from .triggers import TriggerCollection
from .data_processing import DataProcessingCollection
from .metadata import MetadataCollection
from .workflow_templates import WorkflowTemplatesCollection
from .task_templates import TaskTemplatesCollection
from .workflow_instances import WorkflowInstancesCollection
from .task_instances import TaskInstancesCollection


log = logging.getLogger(__name__)


class MongoStorage:

    def __init__(self):
        self.client = None
        self.db = None

        # Collections
        self.workflow_templates = None
        self.task_templates = None
        self.regexes = None
        self.lookups = None
        self.triggers = None
        self.metadata = None
        self.workflow_instances = None
        self.task_instances = None

    def configure(self, host, database, **kwargs):
        log.info("Setting up workflow mongo storage with host '%s'", host)
        self.client = AsyncIOMotorClient(host, **kwargs)
        self.db = self.client[database]
        log.info("Workflow database: '%s'", database)

        # Collections
        self.workflow_templates = WorkflowTemplatesCollection(self)
        self.task_templates = TaskTemplatesCollection(self)
        self.regexes = DataProcessingCollection(self, 'regexes')
        self.lookups = DataProcessingCollection(self, 'lookups')
        self.triggers = TriggerCollection(self)
        self.metadata = MetadataCollection(self)
        self.workflow_instances = WorkflowInstancesCollection(self)
        self.task_instances = TaskInstancesCollection(self)
