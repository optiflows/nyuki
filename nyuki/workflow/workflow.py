import json
import asyncio
import logging
import pickle
import aiohttp
from uuid import uuid4
from copy import deepcopy
from random import shuffle
from datetime import datetime
from tukio import Engine, TaskRegistry, get_broker, EXEC_TOPIC
from tukio.workflow import Workflow, WorkflowExecState
from tukio.task.factory import TaskExecState

from nyuki import Nyuki
from nyuki.memory import memsafe
from nyuki.utils import serialize_object, utcnow
from nyuki.workflow.db.storage import MongoStorage
from nyuki.workflow.db.migrations import run_migrations
from nyuki.workflow.db.task_instances import WS_FILTERS

from .api.factory import (
    ApiFactoryRegex, ApiFactoryRegexes, ApiFactoryLookup, ApiFactoryLookups,
    ApiFactoryLookupCSV
)
from .api.templates import (
    ApiTasks, ApiTemplates, ApiTemplate, ApiTemplateVersion, ApiTemplateDraft
)
from .api.instances import (
    ApiWorkflow, ApiWorkflows, ApiWorkflowsHistory, ApiWorkflowHistory,
    ApiWorkflowTriggers, ApiWorkflowTrigger, ApiWorkflowHistoryTask,
    ApiWorkflowHistoryTaskData, ApiTaskReporting, ApiTaskReportingContact,
    ApiTaskReportingContacts
)
from .api.vars import (
    ApiVars, ApiVarsVersion, ApiVarsDraft
)

from .tasks import *
from .tasks.utils import runtime, CONTACT_PROGRESS
from .tukio import WorkflowSelector


log = logging.getLogger(__name__)


class BadRequestError(Exception):
    pass


@serialize_object.register(Workflow)
def _serialize_workflow(wf):
    """
    Workflow serializer.
    """
    return wf.report()


def sanitize_workflow_exec(obj):
    """
    Replace any object value by 'internal data' string to store in Mongo.
    """
    types = [dict, list, tuple, str, int, float, bool, type(None), datetime]
    if type(obj) not in types:
        obj = 'Internal server data: {}'.format(type(obj))
    elif isinstance(obj, dict):
        for key, value in obj.items():
            obj[key] = sanitize_workflow_exec(value)
    elif isinstance(obj, list):
        for item in obj:
            item = sanitize_workflow_exec(item)
    return obj


class WorkflowInstance:

    """
    Holds a workflow pair of template/instance.
    Allows retrieving a workflow exec state at any moment.
    """

    __slots__ = ('_template', '_instance', '_exec')

    ALLOWED_EXEC_KEYS = ['requester', 'track']

    def __init__(self, template, instance, **kwargs):
        self._template = template
        self._instance = instance
        self._exec = {
            key: kwargs[key]
            for key in kwargs
            if key in self.ALLOWED_EXEC_KEYS
        }

    @property
    def template(self):
        return self._template

    @property
    def instance(self):
        return self._instance

    @property
    def exec(self):
        return self._exec

    def report(self, tasks=True, data=True):
        """
        Merge a workflow exec instance report and its template.
        """
        template = deepcopy(self._template)
        inst = self._instance.report()
        inst['exec'].update(self._exec)

        result = {
            **inst['exec'],
            'template': template,
        }

        if tasks is False:
            del result['template']['graph']
            del result['template']['tasks']
            return result

        tasks = {task['id']: {'template': task} for task in template['tasks']}
        for task_dict in inst['tasks']:
            if not task_dict.get('exec'):
                # Task was never started, create dummy exec dict.
                task_dict['exec'] = {
                    'id': str(uuid4()),
                    'start': None,
                    'end': None,
                    'state': 'not-started',
                    'inputs': None,
                    'outputs': None,
                    'reporting': None
                }

            # Filter out reporting/data if not necessary
            if data is False:
                del task_dict['exec']['reporting']
                del task_dict['exec']['inputs']
                # Leave the necessary task-end informations available
                if task_dict['exec']['outputs']:
                    task_dict['exec']['outputs'] = {
                        key: task_dict['exec']['outputs'][key]
                        for key in WS_FILTERS
                        if key in task_dict['exec']['outputs']
                    }

            # Add execution informations to each task.
            tasks[task_dict['id']].update(task_dict['exec'])

        result['template']['tasks'] = list(tasks.values())
        return result


class WorkflowNyuki(Nyuki):

    """
    Generic workflow nyuki allowing data storage and manipulation
    of tukio's workflows.
    https://github.com/optiflows/tukio
    """

    CONF_SCHEMA = {
        'type': 'object',
        'required': ['mongo'],
        'properties': {
            'mongo': {
                'type': 'object',
                'required': ['host', 'database'],
                'properties': {
                    'host': {'type': 'string', 'minLength': 1},
                    'database': {'type': 'string', 'minLength': 1},
                    'validate_on_start': {'type': 'boolean', 'default': True},
                }
            },
            'topics': {
                'type': 'array',
                'items': {'type': 'string', 'minLength': 1}
            }
        }
    }
    HTTP_RESOURCES = Nyuki.HTTP_RESOURCES + [
        ApiTasks,                   # /v1/workflow/tasks
        ApiTemplates,               # /v1/workflow/templates
        ApiTemplate,                # /v1/workflow/templates/{uid}
        ApiTemplateDraft,           # /v1/workflow/templates/{uid}/draft
        ApiTemplateVersion,         # /v1/workflow/templates/{uid}/{version}
        ApiWorkflows,               # /v1/workflow/instances
        ApiWorkflow,                # /v1/workflow/instances/{uid}
        ApiTaskReporting,           # /v1/workflow/instances/{uid}/tasks/{task_id}/reporting
        ApiTaskReportingContacts,   # /v1/workflow/instances/{uid}/tasks/{task_id}/reporting/contacts
        ApiTaskReportingContact,    # /v1/workflow/instances/{uid}/tasks/{task_id}/reporting/contacts/{contact_id}
        ApiWorkflowsHistory,        # /v1/workflow/history
        ApiWorkflowHistory,         # /v1/workflow/history/{uid}
        ApiWorkflowHistoryTask,     # /v1/workflow/history/{uid}/tasks/{task_id}
        ApiWorkflowHistoryTaskData, # /v1/workflow/history/{uid}/tasks/{task_id}/data
        ApiFactoryRegexes,          # /v1/workflow/regexes
        ApiFactoryRegex,            # /v1/workflow/regexes/{uid}
        ApiFactoryLookups,          # /v1/workflow/lookups
        ApiFactoryLookup,           # /v1/workflow/lookups/{uid}
        ApiFactoryLookupCSV,        # /v1/workflow/lookups/{uid}/csv
        ApiWorkflowTriggers,        # /v1/workflow/triggers
        ApiWorkflowTrigger,         # /v1/workflow/triggers/{tid},
        ApiVars,                    # /v1/workflow/vars/{uid}
        ApiVarsVersion,             # /v1/workflow/vars/{uid}/{version}
        ApiVarsDraft,               # /v1/workflow/data/{uid}/draft
    ]

    DEFAULT_POLICY = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.migrate_config()
        self.register_schema(self.CONF_SCHEMA)
        self.engine = None
        self.storage = MongoStorage()

        self.AVAILABLE_TASKS = {}
        for name, value in TaskRegistry.all().items():
            self.AVAILABLE_TASKS[name] = getattr(value[0], 'SCHEMA', {})

        # Stores workflow instances with their template data
        self.running_workflows = {}

        runtime.bus = self.bus
        runtime.config = self.config
        runtime.workflows = self.running_workflows

    @property
    def mongo_config(self):
        return self.config['mongo']

    @property
    def topics(self):
        return self.config.get('topics', [])

    async def setup(self):
        self.storage.configure(**self.mongo_config)
        # Blocks until connection to Mongo is done.
        await self.storage.index()
        await run_migrations(**self.mongo_config)
        selector = WorkflowSelector(self.storage)
        self.engine = Engine(selector=selector, loop=self.loop)
        for topic in self.topics:
            asyncio.ensure_future(self.bus.subscribe(
                topic, self.workflow_event
            ))
        # Enable workflow exec follow-up
        get_broker().register(self.report_workflow, topic=EXEC_TOPIC)
        # Handle distributed workflow's failures
        if 'raft' in self._services.all:
            self.raft.register('failures', self.failure_handler)

    async def reload(self):
        self.storage.configure(**self.mongo_config)

    async def teardown(self):
        if self.engine:
            await self.engine.stop()

    def new_workflow(self, template, instance, **kwargs):
        """
        Keep in memory a workflow template/instance pair.
        """
        wflow = WorkflowInstance(template, instance, **kwargs)
        self.running_workflows[instance.uid] = wflow
        if 'memory' in self._services and self.memory.available:
            asyncio.ensure_future(
                self.write_report(wflow.report(), False)
            )
        return wflow

    async def report_workflow(self, event):
        """
        Send all worklfow updates to the clients.
        """
        source = event.source.as_dict()
        instance_id = source['workflow_exec_id']
        try:
            wflow = self.running_workflows[instance_id]
        except KeyError:
            log.debug('Outdated event to report: %s', event)
            return

        topic = 'workflow/exec/{}'.format(instance_id)
        payload = {
            'type': event.data['type'],
            'ts': utcnow(),
            'topic': topic,
            'service': self.bus.name,
        }

        # If is a workflow update, send the 'child' value
        if event.data['type'] in WorkflowExecState.values():
            requester = wflow.exec.get('requester')
            if requester:
                payload['requester'] = requester

        # A task information requires the corresponding template id
        # and a more precise topic.
        task_exec_id = source.get('task_exec_id')
        if task_exec_id:
            topic = '{}/tasks/{}'.format(topic, task_exec_id)
            payload['template'] = {'id': source['task_template_id']}

            # Append full data if this is a 'task-progress'
            if event.data['type'] == TaskExecState.PROGRESS.value:
                payload['data'] = event.data.get('content') or {}
                topic = '{}/reporting'.format(topic)

            # Custom event type for single contact updates
            elif event.data['type'] == CONTACT_PROGRESS:
                payload['data'] = event.data.get('content') or {}
                topic = '{}/reporting/contacts/{}'.format(
                    topic, payload['data']['uid'],
                )
                # Remove contact uid (available in topic)
                del payload['data']['uid']

            elif event.data['type'] in (
                TaskExecState.TIMEOUT.value,
                TaskExecState.END.value,
                TaskExecState.ERROR.value,
            ):
                if isinstance(event.data['content'], dict):
                    # Only send the task's important fields, if any
                    payload['data'] = {
                        key: event.data['content'][key]
                        for key in WS_FILTERS
                        if key in event.data['content']
                    }

            # Update topic for this event
            payload['topic'] = topic

        memwrite = True
        # Workflow begins, also send the full template.
        if event.data['type'] == WorkflowExecState.BEGIN.value:
            payload['template'] = dict(wflow.template)
        # Workflow ended, clear it from memory
        elif event.data['type'] in [
            WorkflowExecState.END.value,
            WorkflowExecState.ERROR.value
        ]:
            payload['data'] = event.data.get('content') or {}
            # Sanitize objects to store the finished workflow instance
            asyncio.ensure_future(self.storage.insert_instance(
                sanitize_workflow_exec(wflow.report())
            ))
            del self.running_workflows[instance_id]
            memwrite = False

        # Shared memory set/del
        if 'memory' in self._services and self.memory.available:
            if memwrite:
                memjob = self.write_report(wflow.report())
            else:
                memjob = self.clear_report(instance_id)
            asyncio.ensure_future(memjob)

        await self.bus.publish(payload, 'websocket/{}'.format(topic))

    async def workflow_event(self, topic, data):
        """
        New bus event received, trigger workflows if needed.
        """
        templates = {}
        # Retrieve full workflow templates
        # TODO: Better way to fetch the full template details
        # (this does may more requests than it should)
        wf_templates = await self.engine.selector.select(topic)
        for wftmpl in wf_templates:
            templates[wftmpl.uid] = await self.storage.get_template(
                wftmpl.uid, draft=False
            )
        # Trigger workflows
        instances = await self.engine.data_received(data, topic)
        for instance in instances:
            self.new_workflow(templates[instance.template.uid], instance)

    @memsafe
    async def failure_handler(self, instances):
        """
        This method is called upon failure detection implemented in raft.

        Failing instances are just instances that have been shutdown, either
        because of a crash, or a restart. Let's find out which ones need a
        failover (basically, rescuing workflows detached from dead instances).
        """
        log.debug(
            "The following '%s' instances are failing: %s",
            self.config['service'], instances
        )

        # Select eligible rescuers
        ntw = self.raft.network
        rescuers = [ipv4 for ipv4, uid in ntw.items() if uid not in instances]

        # Iterate over all failing instances
        for ifrom in instances:

            # Fetch the list of workflows for a given failing instance.
            index = self.memory.key(ifrom, 'workflows', 'instances')
            for wflow in await self.memory.store.smembers(index):
                wflow = wflow.decode('utf-8')

                # Get the report shared by the failing instance
                try:
                    report = await self.read_report(wflow, ifrom)
                except KeyError:
                    log.error("Workflow %s memory has been wiped out", wflow)
                    break

                shuffle(rescuers)
                report = json.dumps(report, default=serialize_object)

                # Send a failover request to a valid, not failing, instance.
                for ito in rescuers:
                    request = {
                        'url': 'http://{}:{}/v1/workflow/instances'.format(
                            ito, self.api._port
                        ),
                        'headers': {'Content-Type': 'application/json'},
                        'data': report
                    }
                    async with aiohttp.ClientSession() as session:
                        async with session.put(**request) as resp:
                            if resp.status == 200:
                                # `ito` rescuer has taken over the workflow
                                break
                else:
                    log.error("Workflow %s hasn't be rescued properly", wflow)
                    continue
                asyncio.ensure_future(self.clear_report(wflow, ifrom=ifrom))

    @memsafe
    async def clear_report(self, uid, ifrom=None):
        """
        Remove a report from the shared memory.
        """
        _iform = ifrom or self.id
        await self.memory.store.delete(
            key=self.memory.key(_iform, 'workflows', 'instances', uid)
        )
        await self.memory.store.srem(
            key=self.memory.key(_iform, 'workflows', 'instances'),
            member=uid
        )

    @memsafe
    async def write_report(self, report, replace=True, ito=None):
        """
        Store an instance report into shared memory.
        A simple 'set' is used againts a 'hset' (hash storage), even though the
        'hset' seems more appropriate, because a field in a hash can't have TTL
        """
        _ito = ito or self.id
        uid = report['exec']['id']
        response = await self.memory.store.set(
            key=self.memory.key(_ito, 'workflows', 'instances', uid),
            value=pickle.dumps(report),
            expire=86400,
            exist=None if replace else False
        )

        if not response:
            log.error("Can't share workflow id %s context in memory", uid)
            return

        keyspace = self.memory.key(self.id, 'workflows', 'instances')
        await self.memory.store.sadd(key=keyspace, member=uid)
        await self.memory.store.expire(key=keyspace, timeout=86400)

    @memsafe
    async def read_report(self, uid, ifrom=None):
        """
        Read and parse a report from the shared memory.
        """
        _iform = ifrom or self.id
        report = await self.memory.store.get(
            key=self.memory.key(_iform, 'workflows', 'instances', uid)
        )
        if not report:
            raise KeyError("Can't find workflow id context %s in memory", uid)
        return pickle.loads(report)
