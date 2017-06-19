import json
import asyncio
import logging
import pickle
import aiohttp
from random import shuffle
from datetime import datetime
from tukio import Engine, TaskRegistry, get_broker, EXEC_TOPIC
from tukio.workflow import Workflow, WorkflowTemplate, WorkflowExecState
from tukio.task.factory import TaskExecState

from nyuki import Nyuki
from nyuki.bus import reporting
from nyuki.memory import memsafe
from nyuki.utils import serialize_object, utcnow

from .api.factory import (
    ApiFactoryRegex, ApiFactoryRegexes, ApiFactoryLookup, ApiFactoryLookups,
    ApiFactoryLookupCSV
)
from .api.templates import (
    ApiTasks, ApiTemplates, ApiTemplate, ApiTemplateVersion, ApiTemplateDraft
)
from .api.workflows import (
    WS_FILTERS,
    ApiWorkflow, ApiWorkflows, ApiWorkflowsHistory, ApiWorkflowHistory,
    ApiWorkflowTriggers, ApiWorkflowTrigger, ApiWorkflowHistoryTask,
    ApiWorkflowHistoryTaskData, ApiTaskReporting, ApiTaskReportingContact,
    ApiTaskReportingContacts,
)
from .api.vars import (
    ApiVars, ApiVarsVersion, ApiVarsDraft
)

from .storage import MongoStorage
from .tasks import *
from .tasks.utils import runtime, CONTACT_PROGRESS


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
        template = self._template.copy()
        inst = self._instance.report()
        inst['exec'].update(self._exec)

        result = {
            **self._template,
            'exec': inst['exec'],
        }

        if tasks is False:
            del result['graph']
            del result['tasks']
            return result

        tasks = {task['id']: task for task in template['tasks']}
        for task in inst['tasks']:
            # Filter out reporting/data if not necessary
            if task.get('exec') and data is False:
                del task['exec']['reporting']
                del task['exec']['inputs']
                del task['exec']['outputs']
            # Stored template contains more info than tukio's (title...),
            # so we add it to the report.
            tasks[task['id']].update(**task)
        result['tasks'] = list(tasks.values())
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
                'required': ['host'],
                'properties': {
                    'host': {'type': 'string', 'minLength': 1},
                    'database': {'type': 'string', 'minLength': 1}
                }
            },
            'topics': {
                'type': 'array',
                'items': {'type': 'string', 'minLength': 1}
            }
        }
    }
    HTTP_RESOURCES = Nyuki.HTTP_RESOURCES + [
        ApiTasks,  # /v1/workflows/tasks
        ApiTemplates,  # /v1/workflows/templates
        ApiTemplate,  # /v1/workflows/templates/{uid}
        ApiTemplateDraft,  # /v1/workflows/templates/{uid}/draft
        ApiTemplateVersion,  # /v1/workflows/templates/{uid}/{version}
        ApiWorkflows,  # /v1/workflow/instances
        ApiWorkflow,  # /v1/workflow/instances/{uid}
        ApiTaskReporting,  # /v1/workflow/instances/{uid}/tasks/{task_id}/reporting
        ApiTaskReportingContacts,  # /v1/workflow/instances/{uid}/tasks/{task_id}/reporting/contacts
        ApiTaskReportingContact,  # /v1/workflow/instances/{uid}/tasks/{task_id}/reporting/contacts/{contact_id}
        ApiWorkflowsHistory,  # /v1/workflows/history
        ApiWorkflowHistory,  # /v1/workflows/history/{uid}
        ApiWorkflowHistoryTask,  # /v1/workflows/history/{uid}/tasks/{task_id}
        ApiWorkflowHistoryTaskData,  # /v1/workflows/history/{uid}/tasks/{task_id}/data
        ApiFactoryRegexes,  # /v1/workflows/regexes
        ApiFactoryRegex,  # /v1/workflows/regexes/{uid}
        ApiFactoryLookups,  # /v1/workflows/lookups
        ApiFactoryLookup,  # /v1/workflows/lookups/{uid}
        ApiFactoryLookupCSV,  # /v1/workflows/lookups/{uid}/csv
        ApiWorkflowTriggers,  # /v1/workflows/triggers
        ApiWorkflowTrigger,  # /v1/workflows/triggers/{tid},
        ApiVars,  # /v1/workflows/vars/{uid}
        ApiVarsVersion,  # /v1/workflows/vars/{uid}/{version}
        ApiVarsDraft  # /v1/workflows/data/{uid}/draft
    ]

    DEFAULT_POLICY = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.migrate_config()
        self.register_schema(self.CONF_SCHEMA)
        self.engine = None
        self.storage = None

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
        self.engine = Engine(loop=self.loop)
        asyncio.ensure_future(self.reload_from_storage())
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
        asyncio.ensure_future(self.reload_from_storage())

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
        exec_id = source['workflow_exec_id']
        wflow = self.running_workflows[exec_id]
        topic = 'workflow/exec/{}'.format(exec_id)

        payload = {
            'type': event.data['type'],
            'ts': utcnow(),
            'topic': topic,
            'service': self.bus.name,
            'workflow': {
                'exec_id': exec_id,
                'template_id': source['workflow_template_id'],
            }
        }

        # The requester is required for the frontend
        requester = wflow.exec.get('requester')
        if requester:
            payload['workflow']['requester'] = requester

        # A task information requires the corresponding template_id
        # and a more precise topic.
        task_exec_id = source.get('task_exec_id')
        if task_exec_id:
            topic = '{}/tasks/{}'.format(topic, task_exec_id)
            # Add task source ids
            payload['workflow']['task'] = {
                'exec_id': source['task_exec_id'],
                'template_id': source['task_template_id'],
            }

            # Append full data if this is a 'task-progress'
            if event.data['type'] == TaskExecState.progress.value:
                payload['data'] = event.data.get('content') or {}
                topic = '{}/reporting'.format(topic)

            # Custom event type for single contact updates
            elif event.data['type'] == CONTACT_PROGRESS:
                payload['data'] = event.data.get('content') or {}
                topic = '{}/reporting/contacts/{}'.format(
                    topic, payload['data']['contact'],
                )

            elif event.data['type'] in (
                TaskExecState.end.value,
                TaskExecState.error.value,
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
        if event.data['type'] == WorkflowExecState.begin.value:
            payload['template'] = dict(wflow.template)
        # Workflow ended, clear it from memory
        elif event.data['type'] in [
            WorkflowExecState.end.value,
            WorkflowExecState.error.value
        ]:
            # Sanitize objects to store the finished workflow instance
            asyncio.ensure_future(self.storage.instances.insert(
                sanitize_workflow_exec(wflow.report())
            ))
            del self.running_workflows[exec_id]
            memwrite = False

        # Shared memory set/del
        if 'memory' in self._services and self.memory.available:
            if memwrite:
                memjob = self.write_report(wflow.report())
            else:
                memjob = self.clear_report(exec_id)
            asyncio.ensure_future(memjob)

        asyncio.ensure_future(self.bus.publish(
            payload, 'websocket/{}'.format(topic)
        ))

    async def workflow_event(self, efrom, data):
        """
        New bus event received, trigger workflows if needed.
        """
        templates = {}
        # Retrieve full workflow templates
        wf_templates = self.engine.selector.select(efrom)
        for wftmpl in wf_templates:
            template = await self.storage.templates.get(
                wftmpl.uid,
                draft=False,
                with_metadata=True
            )
            templates[wftmpl.uid] = template[0]
        # Trigger workflows
        instances = await self.engine.data_received(data, efrom)
        for instance in instances:
            self.new_workflow(templates[instance.template.uid], instance)

    async def reload_from_storage(self):
        """
        Check mongo, retrieve and load all templates
        """
        self.storage = MongoStorage(**self.mongo_config)

        templates = await self.storage.templates.get_all(
            full=True,
            latest=True,
            with_metadata=False
        )

        for template in templates:
            try:
                await self.engine.load(WorkflowTemplate.from_dict(template))
            except Exception as exc:
                # Means a bad workflow is in database, report it
                reporting.exception(exc)

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
