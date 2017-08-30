from tukio.task import TaskTemplate
from tukio.workflow import WorkflowTemplate


class WorkflowSelector:

    def __init__(self, storage):
        self.storage = storage

    async def get(self, tmpl_id):
        template = await self.storage.workflow_templates.get_one(
            tmpl_id, draft=False
        )
        return WorkflowTemplate.from_dict(template)

    async def select(self, topic):
        templates = await self.storage.workflow_templates.get_for_topic(topic)
        return [
            WorkflowTemplate.from_dict(template)
            for template in templates
        ]
