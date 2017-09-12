import asyncio
import logging
from aiohttp.web import FileField
from tukio import get_broker, EXEC_TOPIC
from tukio.utils import FutureState
from tukio.workflow import (
    WorkflowTemplate, WorkflowExecState, WorkflowRootTaskError
)
from pymongo.errors import AutoReconnect

from nyuki.api import Response, resource, content_type, HTTPBreak
from nyuki.utils import from_isoformat
from nyuki.workflow.tasks.utils.uri import URI, InvalidWorkflowUri
from nyuki.workflow.db.workflow_instances import Ordering


log = logging.getLogger(__name__)


class _WorkflowResource:

    """
    Share methods between workflow resources
    """

    def register_async_handler(self, async_topic, events, wflow):
        broker = get_broker()
        topic = '/'.join((EXEC_TOPIC, wflow.uid))

        if events:
            events = events.split(',')

        async def exec_handler(event):
            # Publish the event's data if requested.
            if not events or event.data['type'] in events:
                await self.nyuki.bus.publish(event.data, async_topic)
            # If the workflow is in a final state, unregister
            if event.data['type'] in [
                WorkflowExecState.END.value,
                WorkflowExecState.ERROR.value
            ]:
                broker.unregister(exec_handler, topic=topic)

        broker.register(exec_handler, topic=topic)


@resource('/workflow/instances', ['v1'], 'application/json')
class ApiWorkflows(_WorkflowResource):

    async def get(self, request):
        """
        Return workflow instances
        """
        workflows = []
        children = request.GET.get('children', '0') == '1'
        tasks = request.GET.get('tasks', '0') == '1'

        for wflow in self.nyuki.running_workflows.values():
            if children is False:
                requester = wflow.exec.get('requester')
                if requester and requester.startswith('nyuki://'):
                    continue
            workflows.append(wflow.report(tasks=tasks))

        return Response(workflows)

    async def put(self, request):
        """
        Start a workflow from payload:
        {
            "id": "template_id",
            "draft": true/false,
            "exec": {}
        }
        """
        async_topic = request.headers.get('X-Surycat-Async-Topic')
        async_events = request.headers.get('X-Surycat-Async-Events')
        exec_track = request.headers.get('X-Surycat-Exec-Track')
        requester = request.headers.get('Referer')
        request = await request.json()

        if 'id' not in request:
            return Response(status=400, body={
                'error': "Template's ID key 'id' is mandatory"
            })
        draft = request.get('draft', False)
        data = request.get('inputs', {})
        exec = request.get('exec')

        if exec:
            # Suspended/crashed instance
            # The request's payload is the last known execution report
            templates = [request]
            if exec['id'] in self.nyuki.running_workflows:
                return Response(status=400, body={
                    'error': 'This workflow is already being rescued'
                })
        else:
            # Fetch the template from the storage
            try:
                template = await self.nyuki.storage.get_template(
                    request['id'], draft=draft
                )
            except AutoReconnect:
                return Response(status=503)

        if not template:
            return Response(status=404, body={
                'error': 'Could not find a suitable template to run'
            })

        wf_tmpl = WorkflowTemplate.from_dict(template)
        try:
            wf_tmpl.root()
        except WorkflowRootTaskError:
            return Response(status=400, body={
                'error': 'More than one root task'
            })

        if exec:
            wflow = await self.nyuki.engine.rescue(wf_tmpl, request)
        elif draft:
            wflow = await self.nyuki.engine.run_once(wf_tmpl, data)
        else:
            wflow = await self.nyuki.engine.trigger(wf_tmpl.uid, data)

        if wflow is None:
            return Response(status=400, body={
                'error': 'Could not start any workflow from this template'
            })

        # Prevent workflow loop
        exec_track = exec_track.split(',') if exec_track else []
        holder = self.nyuki.bus.name
        for ancestor in exec_track:
            try:
                info = URI.parse(ancestor)
            except InvalidWorkflowUri:
                continue
            if info.template_id == wf_tmpl.uid and info.holder == holder:
                return Response(status=400, body={
                    'error': 'Loop detected between workflows'
                })

        # Keep full instance+template in nyuki's memory
        wfinst = self.nyuki.new_workflow(
            template, wflow,
            track=exec_track,
            requester=requester
        )
        # Handle async workflow exec updates
        if async_topic is not None:
            self.register_async_handler(async_topic, async_events, wflow)

        try:
            # Wait up to 30 seconds for the workflow to start.
            await asyncio.wait_for(wfinst.instance._committed.wait(), 30.0)
        except asyncio.TimeoutError:
            status = 201
        else:
            status = 200

        return Response(wfinst.report(), status=status)


@resource('/workflow/instances/{iid}', versions=['v1'])
class ApiWorkflow:

    async def get(self, request, iid):
        """
        Return a workflow instance
        """
        try:
            return Response(self.nyuki.running_workflows[iid].report(data=False))
        except KeyError:
            return Response(status=404)

    async def post(self, request, iid):
        """
        Suspend/resume a runnning workflow.
        """
        try:
            wf = self.nyuki.running_workflows[iid]
        except KeyError:
            return Response(status=404)

        request = await request.json()

        try:
            action = request['action']
        except KeyError:
            return Response(status=400, body={
                'action parameter required'
            })

        # Should we return 409 Conflict if the status is already set ?
        if action == 'suspend':
            wf.instance.suspend()
        elif action == 'resume':
            wf.instance.resume()
        else:
            return Response(status=400, body={
                "action must be 'suspend' or 'resume'"
            })

        return Response(wf.report())

    async def delete(self, request, iid):
        """
        Cancel a workflow instance.
        """
        try:
            self.nyuki.running_workflows[iid].instance.cancel()
        except KeyError:
            return Response(status=404)


@resource('/workflow/instances/{iid}/tasks/{tid}/reporting', versions=['v1'])
class ApiTaskReporting:

    async def get(self, request, iid, tid):
        """
        Return all the current reporting data for this task.
        """
        try:
            workflow = self.nyuki.running_workflows[iid].instance
            for task in workflow.tasks:
                if task.uid == tid:
                    return Response(task.holder.report() or {})
            else:
                return Response(status=404)
        except KeyError:
            return Response(status=404)


@resource('/workflow/instances/{iid}/tasks/{tid}/reporting/contacts', versions=['v1'])
class ApiTaskReportingContacts:

    async def get(self, request, iid, tid):
        """
        Return all contacts' informations from the reporting data of this task.
        """
        try:
            workflow = self.nyuki.running_workflows[iid].instance
            for task in workflow.tasks:
                if task.uid == tid:
                    if not hasattr(task.holder, 'report_contacts'):
                        raise HTTPBreak(404)
                    return Response(list(task.holder.report_contacts().values()))
            else:
                raise HTTPBreak(404)
        except KeyError:
            raise HTTPBreak(404)


@resource('/workflow/instances/{iid}/tasks/{tid}/reporting/contacts/{cuid}', versions=['v1'])
class ApiTaskReportingContact:

    async def get(self, request, iid, tid, cuid):
        """
        Return one contact's informations from the reporting data of this task.
        """
        try:
            workflow = self.nyuki.running_workflows[iid].instance
            for task in workflow.tasks:
                if task.uid == tid:
                    if not hasattr(task.holder, 'report_contacts'):
                        raise HTTPBreak(404)
                    contacts = task.holder.report_contacts()
                    return Response(contacts[cuid])
            else:
                raise HTTPBreak(404)
        except KeyError:
            raise HTTPBreak(404)


@resource('/workflow/history', versions=['v1'])
class ApiWorkflowsHistory:

    async def get(self, request):
        """
        Filters:
            * `root` return only the root workflows
            * `full` return the full graph and details of all workflows
                * :warning: can be a huge amount of data
            * `since` return the workflows since this date
            * `state` return the workflows on this FutureState
            * `offset` return the worflows from this offset
            * `limit` return this amount of workflows
            * `order` order results following the Ordering enum values
            * `search` search templates with specific title
        """
        # Filter on start date
        since = request.GET.get('since')
        if since:
            try:
                since = from_isoformat(since)
            except ValueError:
                return Response(status=400, body={
                    'error': "Could not parse date '{}'".format(since)
                })
        # Filter on state value
        state = request.GET.get('state')
        if state:
            try:
                state = FutureState(state)
            except ValueError:
                return Response(status=400, body={
                    'error': "Unknown state '{}'".format(state)
                })
        # Skip first items
        offset = request.GET.get('offset')
        if offset:
            try:
                offset = int(offset)
            except ValueError:
                return Response(status=400, body={
                    'error': 'Offset must be an int'
                })
        # Limit max result
        limit = request.GET.get('limit')
        if limit:
            try:
                limit = int(limit)
            except ValueError:
                return Response(status=400, body={
                    'error': 'Limit must be an int'
                })
        order = request.GET.get('ordering')
        if order:
            try:
                order = Ordering[order].value
            except KeyError:
                return Response(status=400, body={
                    'error': 'Ordering must be in {}'.format(Ordering.keys())
                })

        try:
            count, history = await self.nyuki.storage.get_history(
                root=(request.GET.get('root') == '1'),
                full=(request.GET.get('full') == '1'),
                search=request.GET.get('search'),
                order=order,
                offset=offset, limit=limit, since=since, state=state,
            )
        except AutoReconnect:
            return Response(status=503)

        data = {'count': count, 'data': history}
        return Response(data)


@resource('/workflow/history/{uid}', versions=['v1'])
class ApiWorkflowHistory:

    async def get(self, request, uid):
        try:
            workflow = await self.nyuki.storage.get_instance(
                uid, (request.GET.get('full') == '1')
            )
        except AutoReconnect:
            return Response(status=503)
        if not workflow:
            return Response(status=404)
        return Response(workflow)


@resource('/workflow/history/{uid}/tasks/{task_id}', versions=['v1'])
class ApiWorkflowHistoryTask:

    async def get(self, request, uid, task_id):
        try:
            task = await self.nyuki.storage.get_instance_task(
                task_id, (request.GET.get('full') == '1')
            )
        except AutoReconnect:
            return Response(status=503)
        if not task:
            return Response(status=404)
        return Response(task)


@resource('/workflow/history/{uid}/tasks/{task_id}/data', versions=['v1'])
class ApiWorkflowHistoryTaskData:

    async def get(self, request, uid, task_id):
        try:
            task = await self.nyuki.storage.get_instance_task_data(task_id)
        except AutoReconnect:
            return Response(status=503)
        if not task:
            return Response(status=404)
        return Response(task)


@resource('/workflow/triggers', versions=['v1'])
class ApiWorkflowTriggers:

    async def get(self, request):
        """
        Return the list of all trigger forms
        """
        try:
            triggers = await self.nyuki.storage.triggers.get()
        except AutoReconnect:
            return Response(status=503)
        return Response(triggers)

    @content_type('multipart/form-data')
    async def put(self, request):
        """
        Upload a trigger form file
        """
        data = await request.post()
        try:
            form = data['form']
            tid = data['tid']
        except KeyError:
            return Response(status=400, body={
                'error': "'form' and 'tid' are mandatory parameters"
            })
        if not isinstance(form, FileField):
            return Response(status=400, body={
                'error': "'form' field must be a file content"
            })

        content = form.file.read().decode('utf-8')
        try:
            templates = await self.nyuki.storage.get_templates(template_id=tid)
            if not templates:
                return Response(status=404)
            trigger = await self.nyuki.storage.triggers.insert(tid, content)
        except AutoReconnect:
            return Response(status=503)
        return Response(trigger)


@resource('/workflow/triggers/{tid}', versions=['v1'])
class ApiWorkflowTrigger:

    async def get(self, request, tid):
        """
        Return a single trigger form
        """
        try:
            trigger = await self.nyuki.storage.triggers.get_one(tid)
        except AutoReconnect:
            return Response(status=503)
        if not trigger:
            return Response(status=404)
        return Response(trigger)

    async def delete(self, request, tid):
        """
        Delete a trigger form
        """
        try:
            trigger = await self.nyuki.storage.triggers.get_one(tid)
        except AutoReconnect:
            return Response(status=503)
        if not trigger:
            return Response(status=404)

        await self.nyuki.storage.triggers.delete(tid)
        return Response(trigger)
