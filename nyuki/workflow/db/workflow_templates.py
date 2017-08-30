import asyncio
import logging
from pymongo import DESCENDING
from pymongo.errors import DuplicateKeyError


log = logging.getLogger(__name__)


class WorkflowTemplateCollection:

    """
    Holds all the templates created for tukio, with their versions.
    These records will be used to ensure a persistence of created workflows
    in case the nyuki get into trouble.
    Templates are retrieved and loaded at startup.
    """

    def __init__(self, storage):
        self._storage = storage
        self._templates = storage.db['templates']
        asyncio.ensure_future(self.index())

    async def index(self):
        await self._templates.create_index(
            [('id', DESCENDING), ('version', DESCENDING)],
            unique=True
        )
        await self._templates.create_index(
            [('id', DESCENDING), ('draft', DESCENDING)]
        )

    async def get_all(self, full=False, with_metadata=True):
        """
        Return all latest and draft templates
        Used at nyuki's startup and GET /v1/templates
        """
        filters = {'_id': 0}
        # '/v1/workflow/templates' does not requires all the informations
        if full is False:
            filters.update({'id': 1, 'draft': 1, 'version': 1, 'topics': 1})

        # Retrieve only the published and the drafts
        cursor = self._templates.find(
            {'draft': {'$in': [True, False]}},
            filters,
        )
        templates = await cursor.to_list(None)

        # Collect metadata
        if with_metadata and templates:
            metadatas = await self._storage.metadata.get([tmpl['id'] for tmpl in templates])
            metadatas = {meta['id']: meta for meta in metadatas}
            for template in templates:
                template.update(metadatas[template['id']])

        return templates

    async def get_one(self, tid, version=None, draft=False, with_metadata=True):
        """
        Return a template's configuration and versions
        """
        if version is not None:
            query = {'id': tid, 'version': int(version)}
        else:
            query = {'id': tid, 'draft': draft}

        template = await self._templates.find_one(query, {'_id': 0})

        # Collect metadata
        if with_metadata and template:
            metadata = await self._storage.metadata.get_one(tid)
            if metadata:
                template.update(metadata)

        return template

    async def get_last_version(self, tid):
        """
        Return the highest version of a template
        """
        query = {'id': tid, 'draft': False}
        cursor = self._templates.find(query)
        cursor.sort('version', DESCENDING)
        await cursor.fetch_next

        template = cursor.next_object()
        return template['version'] if template else 0

    async def insert(self, template):
        """
        Insert a template dict, not updatable
        """
        query = {
            'id': template['id'],
            'version': template['version']
        }

        # Remove draft if any
        await self.delete(template['id'], template['version'], True)

        log.info('Insert template with query: %s', query)
        # Copy dict, mongo somehow alter the given dict
        await self._templates.insert_one(template.copy())

    async def insert_draft(self, template):
        """
        Check and insert draft, updatable
        """
        query = {
            'id': template['id'],
            'draft': True
        }

        log.info('Update draft for query: %s', query)
        await self._templates.replace_one(query, template, upsert=True)

    async def publish_draft(self, tid):
        """
        Set the last published template (draft: False) to None
        Set the draft template (draft: True) to False
        """
        await self._templates.update_one(
            {'id': tid, 'draft': False},
            {'$set': {'draft': None}},
        )
        await self._templates.update_one(
            {'id': tid, 'draft': True},
            {'$set': {'draft': False}},
        )

    async def delete(self, tid, version=None, draft=None):
        """
        Delete a template from its id with all its versions
        """
        query = {'id': tid}
        if version:
            query['version'] = version
        if draft is not None:
            query['draft'] = draft

        log.info("Removing template(s) with query: %s", query)

        await self._templates.delete_many(query)
        left = await self._templates.find({'id': tid}).count()
        if not left:
            await self._storage.metadata.delete(tid)
