import asyncio
import logging
from pymongo import DESCENDING
from pymongo.errors import DuplicateKeyError


log = logging.getLogger(__name__)


class TemplateCollection:

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

    async def get_all(self, full=False, latest=False, draft=False, with_metadata=True):
        """
        Return all templates, used at nyuki's startup and GET /v1/templates
        Fetch latest versions if latest=True
        Fetch drafts if draft=True
        Both drafts and latest version if both are True
        """
        filters = {'_id': 0}
        # '/v1/workflow/templates' does not requires all the informations
        if full is False:
            filters.update({'id': 1, 'draft': 1, 'version': 1, 'topics': 1})

        cursor = self._templates.find(None, filters)
        cursor.sort('version', DESCENDING)
        templates = await cursor.to_list(None)

        # Collect metadata
        if with_metadata and templates:
            metadatas = await self._storage.metadata.get([tmpl['id'] for tmpl in templates])
            metadatas = {meta['id']: meta for meta in metadatas}
            for template in templates:
                template.update(metadatas[template['id']])

        if latest is False and draft is False:
            return templates

        # Retrieve the latest versions + drafts
        lasts = {}
        drafts = []

        for template in templates:
            if draft and template['draft']:
                drafts.append(template)
            elif latest and not template['draft'] and template['id'] not in lasts:
                lasts[template['id']] = template

        return drafts + list(lasts.values())

    async def get(self, tid, version=None, draft=None, with_metadata=True):
        """
        Return a template's configuration and versions
        """
        query = {'id': tid}

        if version:
            query['version'] = int(version)
        if draft is not None:
            query['draft'] = draft

        cursor = self._templates.find(query, {'_id': 0})
        cursor.sort('version', DESCENDING)
        templates = await cursor.to_list(None)

        # Collect metadata
        if with_metadata and templates:
            metadata = await self._storage.metadata.get_one(tid)
            if metadata:
                for template in templates:
                    template.update(metadata)

        return templates

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
        From draft to production
        """
        query = {'id': tid, 'draft': True}
        await self._templates.update_one(query, {'$set': {'draft': False}})

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
