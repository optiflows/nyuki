import asyncio
import logging


log = logging.getLogger(__name__)


class MetadataCollection:

    def __init__(self, storage):
        self._metadata = storage.db['metadata']
        asyncio.ensure_future(self.index())

    async def index(self):
        await self._metadata.create_index('id', unique=True)

    async def get(self, tids=None):
        """
        Return metadata for all or part of the templates.
        """
        if isinstance(tids, list):
            query = {'id': {'$in': tids}}
        else:
            query = None

        cursor = self._metadata.find(query, {'_id': 0})
        return await cursor.to_list(None)

    async def get_one(self, tid):
        """
        Return metadata for one template.
        """
        return await self._metadata.find_one({'id': tid}, {'_id': 0})

    async def insert(self, metadata):
        """
        Insert new metadata for a template.
        """
        query = {'id': metadata['id']}

        metadata = {
            'id': metadata['id'],
            'title': metadata.get('title', ''),
            'tags': metadata.get('tags', [])
        }

        await self._metadata.replace_one(query, metadata, upsert=True)
        log.info(
            'Inserted metadata for template %s (%s)',
            metadata['id'][:8], metadata['title'],
        )
        return metadata

    async def delete(self, tid):
        """
        Delete metadata for one template.
        """
        await self._metadata.delete_one({'id': tid})
