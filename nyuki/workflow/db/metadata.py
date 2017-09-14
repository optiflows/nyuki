import logging

from pymongo import ReturnDocument


log = logging.getLogger(__name__)


class MetadataCollection:

    """
    {
        "workflow_template_id": <uuid4>,
        "title": <str>,
        "tags": [<str>]
    }
    """

    def __init__(self, db):
        self._metadata = db['workflow_metadata']

    async def index(self):
        await self._metadata.create_index('workflow_template_id', unique=True)

    async def get_one(self, tid):
        """
        Return metadata for one template.
        """
        return await self._metadata.find_one(
            {'workflow_template_id': tid},
            {'_id': 0, 'workflow_template_id': 0},
        )

    async def insert(self, metadata):
        """
        Insert new metadata for a template.
        """
        await self._metadata.insert_one(metadata)
        del metadata['_id']
        return metadata

    async def update(self, tid, metadata):
        """
        Update and return the updated metadata.
        """
        return await self._metadata.find_one_and_update(
            {'workflow_template_id': tid},
            {'$set': {
                key: value
                for key, value in metadata.items()
                if value is not None
            }},
            projection={'_id': 0},
            return_document=ReturnDocument.AFTER,
        )

    async def delete(self, tid):
        """
        Delete metadata for one template.
        """
        await self._metadata.delete_one({'workflow_template_id': tid})
