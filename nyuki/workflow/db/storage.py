import sys
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient

from .triggers import TriggerCollection
from .data_processing import DataProcessingCollection
from .metadata import MetadataCollection
from .workflow_templates import WorkflowTemplateCollection
from .workflow_instances import WorkflowInstanceCollection
from .task_instances import TaskInstanceCollection


log = logging.getLogger(__name__)


class MongoStorage:

    DEFAULT_DATABASE = 'workflow'

    def __init__(self):
        self.client = None
        self.db = None

        # Collections
        self.templates = None
        self.regexes = None
        self.lookups = None
        self.triggers = None
        self.metadata = None
        self.workflow_instances = None
        self.task_instances = None

    def configure(self, host, database=None, **kwargs):
        log.info("Setting up workflow mongo storage with host '%s'", host)
        self.client = AsyncIOMotorClient(host, **kwargs)
        db_name = database or self.DEFAULT_DATABASE
        self.db = self.client[db_name]
        log.info("Workflow database: '%s'", db_name)

        # Collections
        self.templates = WorkflowTemplateCollection(self)
        self.regexes = DataProcessingCollection(self, 'regexes')
        self.lookups = DataProcessingCollection(self, 'lookups')
        self.triggers = TriggerCollection(self)
        self.metadata = MetadataCollection(self)
        self.workflow_instances = WorkflowInstanceCollection(self)
        self.task_instances = TaskInstanceCollection(self)
