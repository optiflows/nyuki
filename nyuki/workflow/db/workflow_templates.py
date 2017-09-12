import asyncio
import logging
from enum import Enum
from pymongo import DESCENDING


log = logging.getLogger(__name__)


class TemplateState(Enum):

    DRAFT = 'draft'
    ACTIVE = 'active'
    ARCHIVED = 'archived'

    @classmethod
    def active_states(cls):
        return [cls.DRAFT.value, cls.ACTIVE.value]

    @classmethod
    def draft_state(cls, draft):
        return cls.DRAFT.value if draft is True else cls.ACTIVE.value


class WorkflowTemplatesCollection:

    """
    Holds all the templates created for tukio, with their versions.
    These records will be used to ensure a persistence of created workflows
    in case the nyuki get into trouble.
    Templates are retrieved and loaded at startup.

    {
        "id": <uuid4>,
        "policy": <str>,
        "topics": [<str>],
        "graph": {},
        "version": <int>,
        "state": <draft | active | archived>
    }
    """

    def __init__(self, db):
        self._templates = db['workflow_templates']

    async def index(self):
        await self._templates.create_index('topics')
        await self._templates.create_index(
            [('id', DESCENDING), ('version', DESCENDING)],
            unique=True
        )
        await self._templates.create_index(
            [('id', DESCENDING), ('state', DESCENDING)]
        )

    async def get(self, template_id=None, full=False):
        """
        Return all active and draft templates
        Used at nyuki's startup and GET /v1/templates
        """
        query = {'state': {'$in': TemplateState.active_states()}}
        if template_id is not None:
            query['id'] = template_id
        filters = {'_id': 0}
        if full is False:
            filters.update({'id': 1, 'state': 1, 'version': 1, 'topics': 1})

        # Retrieve only the actives and the drafts
        cursor = self._templates.find(query, filters)
        return await cursor.to_list(None)

    async def get_one(self, tid, version=None, draft=False):
        """
        Return a template's configuration and versions
        """
        if version is not None:
            # We ask for a specific version, regardless of its state.
            query = {'id': tid, 'version': int(version)}
        else:
            # Else, we ask for either the active version or the draft.
            query = {
                'id': tid,
                'state': TemplateState.draft_state(draft),
            }

        return await self._templates.find_one(query, {'_id': 0})

    async def get_for_topic(self, topic):
        """
        Return the latest templates (non-draft) that wait
        for a certain topic.
        """
        query = {
            '$or': [
                {'topics': topic, 'state': TemplateState.ACTIVE.value},
                {'topics': None, 'state': TemplateState.ACTIVE.value},
            ]
        }
        cursor = self._templates.find(query, {'_id': 0})
        return await cursor.to_list(None)

    async def get_last_version(self, tid):
        """
        Return the highest version of a template
        """
        query = {'id': tid, 'state': TemplateState.ACTIVE.value}
        template = await self._templates.find_one(query, {'version': 1})
        return template['version'] if template else 0

    async def insert_draft(self, template):
        """
        Insert or replace a template document.
        This is used to update a draft.
        """
        query = {
            'id': template['id'],
            'state': TemplateState.DRAFT.value,
        }
        await self._templates.replace_one(query, template, upsert=True)

    async def publish_draft(self, tid):
        """
        Set the last published template to 'archived'.
        Set the draft of this template to 'active'.
        """
        await self._templates.update_one(
            {'id': tid, 'state': TemplateState.ACTIVE.value},
            {'$set': {'state': TemplateState.ARCHIVED.value}},
        )
        await self._templates.update_one(
            {'id': tid, 'state': TemplateState.DRAFT.value},
            {'$set': {'state': TemplateState.ACTIVE.value}},
        )

    async def delete(self, tid, draft=False):
        """
        Delete a template with all its versions.
        Delete only the draft if specified.
        """
        if draft is False:
            # Delete all template versions.
            await self._templates.delete_many({'id': tid})
            return

        # Delete only the draft.
        await self._templates.delete_one({
            'id': tid,
            'state': TemplateState.DRAFT.value,
        })
