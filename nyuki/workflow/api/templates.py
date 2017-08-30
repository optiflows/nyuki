import asyncio
import logging
from pymongo.errors import AutoReconnect, DuplicateKeyError
from tukio.workflow import TemplateGraphError, WorkflowTemplate

from nyuki.api import Response, resource
from nyuki.workflow.validation import validate, TemplateError


log = logging.getLogger(__name__)


@resource('/workflow/tasks', versions=['v1'])
class ApiTasks:

    async def get(self, request):
        """
        Return the available tasks
        """
        return Response(self.nyuki.AVAILABLE_TASKS)


class _TemplateResource:

    """
    Share methods between templates resources
    """

    async def update_draft(self, template, from_request):
        """
        Helper to insert/update a draft
        """
        tmpl_dict = template.as_dict()

        # Auto-increment version, draft only
        last_version = await self.nyuki.storage.workflow_templates.get_last_version(
            template.uid
        )
        tmpl_dict['version'] = last_version + 1
        tmpl_dict['draft'] = True

        # Store task extra info (ie. title)
        rqst_tasks = from_request.get('tasks', [])
        tmpl_tasks = tmpl_dict['tasks']
        for src in rqst_tasks:
            match = list(filter(lambda t: t['id'] == src['id'], tmpl_tasks))
            if match:
                match[0].update({'title': src.get('title')})

        try:
            template = await self.nyuki.storage.workflow_templates.insert_draft(
                tmpl_dict
            )
        except DuplicateKeyError as exc:
            raise DuplicateKeyError('Template already exists for this version') from exc

        return template

    def errors_from_validation(self, template):
        """
        Validate and return the list of errors if any
        """
        errors = None
        try:
            validate(template)
        except TemplateError as err:
            errors = err.as_dict()
        return errors


@resource('/workflow/templates', versions=['v1'])
class ApiTemplates(_TemplateResource):

    async def get(self, request):
        """
        Return available workflows' DAGs
        """
        try:
            templates = await self.nyuki.storage.workflow_templates.get(
                full=(request.GET.get('full') == '1'),
            )
        except AutoReconnect:
            return Response(status=503)
        return Response(templates)

    async def put(self, request):
        """
        Create a workflow DAG from JSON
        """
        request = await request.json()

        if 'id' in request:
            try:
                draft = await self.nyuki.storage.workflow_templates.get_one(
                    request['id'], draft=True
                )
            except AutoReconnect:
                return Response(status=503)
            if draft:
                return Response(status=409, body={
                    'error': 'draft already exists'
                })

        if self.nyuki.DEFAULT_POLICY is not None and 'policy' not in request:
            request['policy'] = self.nyuki.DEFAULT_POLICY

        try:
            template = WorkflowTemplate.from_dict(request)
        except TemplateGraphError as exc:
            return Response(status=400, body={
                'error': str(exc)
            })

        try:
            metadata = await self.nyuki.storage.metadata.get_one(template.uid)
        except AutoReconnect:
            return Response(status=503)

        if not metadata:
            if 'title' not in request:
                return Response(status=400, body={
                    'error': "workflow 'title' key is mandatory"
                })

            metadata = {
                'id': template.uid,
                'title': request['title'],
                'tags': request.get('tags', [])
            }

            await self.nyuki.storage.metadata.insert(metadata)

        try:
            tmpl_dict = await self.update_draft(template, request)
        except DuplicateKeyError as exc:
            return Response(status=409, body={
                'error': exc
            })

        return Response({
            **tmpl_dict,
            **metadata,
            'errors': self.errors_from_validation(template)
        })


@resource('/workflow/templates/{tid}', versions=['v1'])
class ApiTemplate(_TemplateResource):

    async def get(self, request, tid):
        """
        Return the latest version of the template
        """
        try:
            tmpl = await self.nyuki.storage.workflow_templates.get_one(tid)
        except AutoReconnect:
            return Response(status=503)

        if not tmpl:
            # Check if a draft is available
            tmpl = await self.nyuki.storage.workflow_templates.get_one(tid, draft=True)
            if not tmpl:
                return Response(status=404)

        return Response(tmpl)

    async def put(self, request, tid):
        """
        Create a new draft for this template id
        """
        try:
            draft = await self.nyuki.storage.workflow_templates.get_one(tid, draft=True)
        except AutoReconnect:
            return Response(status=503)
        if draft:
            return Response(status=409, body={
                'error': 'This draft already exists'
            })

        tmpl = await self.nyuki.storage.workflow_templates.get_one(tid, draft=False)
        if not tmpl:
            return Response(status=404)

        request = await request.json()

        try:
            # Set template ID from url
            template = WorkflowTemplate.from_dict({**request, 'id': tid})
        except TemplateGraphError as exc:
            return Response(status=400, body={
                'error': str(exc)
            })

        try:
            tmpl_dict = await self.update_draft(template, request)
        except DuplicateKeyError as exc:
            return Response(status=409, body={
                'error': exc
            })

        metadata = await self.nyuki.storage.metadata.get_one(template.uid)

        return Response({
            **tmpl_dict,
            **metadata,
            'errors': self.errors_from_validation(template)
        })

    async def patch(self, request, tid):
        """
        Modify the template's metadata
        """
        try:
            tmpl = await self.nyuki.storage.workflow_templates.get_one(tid)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        request = await request.json()

        # Add ID, request dict cleaned in storage
        metadata = await self.nyuki.storage.metadata.insert({
            **request,
            'id': tid,
        })

        return Response(metadata)

    async def delete(self, request, tid):
        """
        Delete the template
        """
        try:
            tmpl = await self.nyuki.storage.workflow_templates.get_one(tid)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        await self.nyuki.storage.workflow_templates.delete(tid)
        await self.nyuki.storage.triggers.delete(tid)

        return Response(tmpl)


@resource('/workflow/templates/{tid}/{version:\d+}', versions=['v1'])
class ApiTemplateVersion(_TemplateResource):

    async def get(self, request, tid, version):
        """
        Return the template's given version
        """
        try:
            tmpl = await self.nyuki.storage.workflow_templates.get_one(tid, version=version)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        return Response(tmpl)

    async def delete(self, request, tid, version):
        """
        Delete a template with given version
        """
        try:
            tmpl = await self.nyuki.storage.workflow_templates.get_one(tid)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        await self.nyuki.storage.workflow_templates.delete(tid, version)
        return Response(tmpl)


@resource('/workflow/templates/{tid}/draft', versions=['v1'])
class ApiTemplateDraft(_TemplateResource):

    async def get(self, request, tid):
        """
        Return the template's draft, if any
        """
        try:
            tmpl = await self.nyuki.storage.workflow_templates.get_one(tid, draft=True)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        return Response(tmpl)

    async def post(self, request, tid):
        """
        Publish a draft into production
        """
        try:
            tmpl = await self.nyuki.storage.workflow_templates.get_one(tid, draft=True)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        tmpl['draft'] = False

        try:
            # Set template ID from url
            template = WorkflowTemplate.from_dict(tmpl)
        except TemplateGraphError as exc:
            return Response(status=400, body={
                'error': str(exc)
            })

        errors = self.errors_from_validation(template)
        if errors is not None:
            return Response(status=400, body=errors)

        # Update draft into a new template
        await self.nyuki.storage.workflow_templates.publish_draft(tid)
        return Response(tmpl)

    async def patch(self, request, tid):
        """
        Modify the template's draft
        """
        try:
            tmpl = await self.nyuki.storage.workflow_templates.get_one(tid, draft=True)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        request = await request.json()

        try:
            # Set template ID from url
            template = WorkflowTemplate.from_dict({**request, 'id': tid})
        except TemplateGraphError as exc:
            return Response(status=400, body={
                'error': str(exc)
            })

        try:
            tmpl_dict = await self.update_draft(template, request)
        except DuplicateKeyError as exc:
            return Response(status=409, body={
                'error': str(exc)
            })

        metadata = await self.nyuki.storage.metadata.get_one(template.uid)

        return Response({
            **tmpl_dict,
            **metadata,
            'errors': self.errors_from_validation(template)
        })

    async def delete(self, request, tid):
        """
        Delete the template's draft
        """
        try:
            tmpl = await self.nyuki.storage.workflow_templates.get_one(tid, draft=True)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        await self.nyuki.storage.workflow_templates.delete(tid, draft=True)
        return Response(tmpl)
