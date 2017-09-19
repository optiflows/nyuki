import logging
from pymongo.errors import AutoReconnect, DuplicateKeyError
from tukio.workflow import TemplateGraphError, WorkflowTemplate

from nyuki.api import Response, resource
from nyuki.workflow.validation import validate, TemplateError
from nyuki.workflow.db.workflow_templates import TemplateState


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

    async def upsert_draft(self, template, request):
        """
        Helper to insert/update a draft.
        """
        template = {
            **template.as_dict(),
            'title': request['title'],
            'tags': request.get('tags', []),
        }

        # Store task extra info (ie. title)
        rqst_tasks = request.get('tasks', [])
        tmpl_tasks = template['tasks']
        for src in rqst_tasks:
            match = list(filter(lambda t: t['id'] == src['id'], tmpl_tasks))
            if match:
                match[0].update({'title': src.get('title')})

        try:
            return await self.nyuki.storage.upsert_draft(template)
        except DuplicateKeyError as exc:
            raise DuplicateKeyError('Template already exists for this version') from exc

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
            templates = await self.nyuki.storage.get_templates(
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
            del request['id']

        if 'title' not in request:
            return Response(status=400, body={
                'error': "workflow 'title' is mandatory"
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
            tmpl_dict = await self.upsert_draft(template, request)
        except DuplicateKeyError as exc:
            return Response(status=409, body={
                'error': exc
            })

        tmpl_dict['errors'] = self.errors_from_validation(template)
        return Response(tmpl_dict)


@resource('/workflow/templates/{tid}', versions=['v1'])
class ApiTemplate(_TemplateResource):

    async def get(self, request, tid):
        """
        Return the latest version of the template
        """
        try:
            tmpl = await self.nyuki.storage.get_templates(tid, full=True)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)
        return Response(tmpl)

    async def put(self, request, tid):
        """
        Create a new draft for this template id.
        """
        try:
            tmpl = await self.nyuki.storage.get_template(tid)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        draft = await self.nyuki.storage.get_template(tid, draft=True)
        if draft:
            return Response(status=409, body={
                'error': 'This draft already exists'
            })

        request = await request.json()

        try:
            # Set template ID from url
            template = WorkflowTemplate.from_dict({**request, 'id': tid})
        except TemplateGraphError as exc:
            return Response(status=400, body={
                'error': str(exc)
            })

        try:
            tmpl_dict = await self.upsert_draft(template, request)
        except DuplicateKeyError as exc:
            return Response(status=409, body={
                'error': exc
            })

        tmpl_dict['errors'] = self.errors_from_validation(template)
        return Response(tmpl_dict)

    async def patch(self, request, tid):
        """
        Modify the template's metadata
        """
        try:
            templates = await self.nyuki.storage.get_templates(template_id=tid)
        except AutoReconnect:
            return Response(status=503)
        if not templates:
            return Response(status=404)

        request = await request.json()

        # Add ID, request dict cleaned in storage
        metadata = await self.nyuki.storage.update_workflow_metadata(tid, {
            'title': request.get('title'),
            'tags': request.get('tags', []),
        })

        return Response(metadata)

    async def delete(self, request, tid):
        """
        Delete the template and all its versions.
        """
        try:
            templates = await self.nyuki.storage.get_templates(
                template_id=tid, full=True
            )
        except AutoReconnect:
            return Response(status=503)
        if not templates:
            return Response(status=404)

        await self.nyuki.storage.delete_template(tid)
        return Response(templates)


@resource('/workflow/templates/{tid}/{version:\d+}', versions=['v1'])
class ApiTemplateVersion(_TemplateResource):

    async def get(self, request, tid, version):
        """
        Return the template's given version
        """
        try:
            tmpl = await self.nyuki.storage.get_template(tid, version=version)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        return Response(tmpl)


@resource('/workflow/templates/{tid}/draft', versions=['v1'])
class ApiTemplateDraft(_TemplateResource):

    async def get(self, request, tid):
        """
        Return the template's draft, if any
        """
        try:
            tmpl = await self.nyuki.storage.get_template(tid, draft=True)
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
            tmpl_dict = await self.nyuki.storage.get_template(tid, draft=True)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl_dict:
            return Response(status=404)

        try:
            # Set template ID from url
            template = WorkflowTemplate.from_dict(tmpl_dict)
        except TemplateGraphError as exc:
            return Response(status=400, body={
                'error': str(exc)
            })

        errors = self.errors_from_validation(template)
        if errors is not None:
            return Response(errors, status=400)

        # Update draft into a new template
        await self.nyuki.storage.publish_draft(tid)
        tmpl_dict['state'] = TemplateState.ACTIVE.value
        return Response(tmpl_dict)

    async def patch(self, request, tid):
        """
        Modify the template's draft
        """
        try:
            tmpl = await self.nyuki.storage.get_template(tid, draft=True)
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
            tmpl_dict = await self.upsert_draft(template, request)
        except DuplicateKeyError as exc:
            return Response(status=409, body={
                'error': str(exc)
            })

        tmpl_dict['errors'] = self.errors_from_validation(template)
        return Response(tmpl_dict)

    async def delete(self, request, tid):
        """
        Delete the template's draft
        """
        try:
            tmpl = await self.nyuki.storage.get_template(tid, draft=True)
        except AutoReconnect:
            return Response(status=503)
        if not tmpl:
            return Response(status=404)

        await self.nyuki.storage.delete_template(tid, draft=True)
        return Response(tmpl)
