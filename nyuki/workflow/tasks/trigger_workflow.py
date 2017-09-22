import json
import asyncio
import logging
from enum import Enum
from aiohttp import ClientSession
from tukio.task import register
from tukio.task.holder import TaskHolder
from tukio.workflow import WorkflowExecState, Workflow

from .utils import runtime
from .utils.uri import URI


log = logging.getLogger(__name__)


class WorkflowStatus(Enum):

    PENDING = 'pending'
    RUNNING = 'running'
    TIMEOUT = 'timeout'
    DONE = 'done'


@register('trigger_workflow', 'execute')
class TriggerWorkflowTask(TaskHolder):

    __slots__ = (
        'template', 'blocking', 'task', '_engine', 'data',
        'status', 'triggered_id', 'async_future',
    )

    SCHEMA = {
        'type': 'object',
        'required': ['template'],
        'additionalProperties': False,
        'properties': {
            'template': {
                'type': 'object',
                'required': ['service', 'id'],
                'additionalProperties': False,
                'properties': {
                    'service': {'type': 'string', 'minLength': 1},
                    'id': {'type': 'string', 'minLength': 1},
                    'draft': {'type': 'boolean', 'default': False},
                },
            },
            'blocking': {'type': 'boolean', 'default': True},
        },
    }

    def __init__(self, config):
        super().__init__(config)
        self.template = self.config['template']
        self.blocking = self.config.get('blocking', True)
        self.task = None
        self._engine = 'http://{}/{}/api/v1/workflow'.format(
            runtime.config.get('http_host', 'localhost'),
            self.template['service'],
        )

        # Reporting
        self.status = WorkflowStatus.PENDING.value
        self.data = None
        self.triggered_id = None
        self.async_future = None

    def report(self):
        return {
            'exec_id': self.triggered_id,
            'status': self.status,
        }

    async def async_exec(self, topic, data):
        log.debug(
            "Received data for async trigger_workflow in '%s': %s",
            topic, data,
        )
        if not self.async_future.done():
            self.async_future.set_result(data)
        await runtime.bus.unsubscribe(topic)

    async def execute(self, event):
        """
        Entrypoint execution method.
        """
        self.data = event.data
        self.task = asyncio.Task.current_task()
        is_draft = self.template.get('draft', False)

        # Send the HTTP request
        log.info('Triggering template %s%s on service %s', self.template['id'],
                 ' (draft)' if is_draft else '', self.template['service'])

        # Setup headers (set requester and exec-track to avoid workflow loops)
        workflow = runtime.workflows[Workflow.current_workflow().uid]
        parent = workflow.exec.get('requester')
        track = list(workflow.exec.get('track', []))
        if parent:
            track.append(parent)

        headers = {
            'Content-Type': 'application/json',
            'Referer': URI.instance(workflow.instance),
            'X-Surycat-Exec-Track': ','.join(track)
        }

        # Handle blocking trigger_workflow using mqtt
        if self.blocking:
            topic = '{}/async/{}'.format(runtime.bus.name, self.uid[:8])
            headers['X-Surycat-Async-Topic'] = topic
            headers['X-Surycat-Async-Events'] = ','.join([
                WorkflowExecState.END.value,
                WorkflowExecState.ERROR.value,
            ])
            self.async_future = asyncio.Future()
            await runtime.bus.subscribe(topic, self.async_exec)

            def _unsub(f):
                asyncio.ensure_future(runtime.bus.unsubscribe(topic))
            self.task.add_done_callback(_unsub)

        async with ClientSession() as session:
            # Compute data to send to sub-workflows
            url = '{}/vars/{}{}'.format(
                self._engine,
                self.template['id'],
                '/draft' if is_draft else '',
            )
            async with session.get(url) as response:
                if response.status != 200:
                    raise RuntimeError("Can't load template info")
                wf_vars = await response.json()
            lightened_data = {
                key: self.data[key]
                for key in wf_vars
                if key in self.data
            }

            params = {
                'url': '{}/instances'.format(self._engine),
                'headers': headers,
                'data': json.dumps({
                    'id': self.template['id'],
                    'draft': is_draft,
                    'inputs': lightened_data,
                })
            }
            async with session.put(**params) as response:
                if response.status != 200:
                    log.critical(await response.text())
                    msg = "Can't process workflow template {} on {}".format(
                        self.template, self.nyuki_api
                    )
                    if response.status % 400 < 100:
                        reason = await response.json()
                        msg = "{}, reason: {}".format(msg, reason['error'])
                    raise RuntimeError(msg)
                resp_body = await response.json()
                self.triggered_id = resp_body['id']

        wf_id = '@'.join([self.triggered_id[:8], self.template['service']])
        self.status = WorkflowStatus.RUNNING.value
        log.info('Successfully started %s', wf_id)
        self.task.dispatch_progress(self.report())

        # Block until task completed
        if self.blocking:
            log.info('Waiting for workflow %s to complete', wf_id)
            await self.async_future
            self.status = WorkflowStatus.DONE.value
            log.info('Workflow %s is done', wf_id)
            self.task.dispatch_progress({'status': self.status})

        return self.data

    async def _end_triggered_workflow(self):
        """
        Asynchronously cancel the triggered workflow.
        """
        wf_id = '@'.join([self.triggered_id[:8], self.template['service']])
        async with ClientSession() as session:
            url = '{}/instances/{}'.format(self._engine, self.triggered_id)
            async with session.delete(url) as response:
                if response.status != 200:
                    log.warning('Failed to cancel workflow %s', wf_id)
                else:
                    log.info('Workflow %s has been cancelled', wf_id)

    def teardown(self):
        """
        Called when this task is cancelled or timed out.
        """
        if self.task.timed_out is True:
            self.status = WorkflowStatus.TIMEOUT.value
        else:
            self.status = WorkflowStatus.DONE.value

        self.task.dispatch_progress({'status': self.status})
        if not self.triggered_id:
            log.debug('No workflow to cancel')
            return self.data

        asyncio.ensure_future(self._end_triggered_workflow())
        return self.data
