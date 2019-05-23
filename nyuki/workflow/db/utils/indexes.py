import logging


log = logging.getLogger(__name__)


async def check_index_names(collection, names):
    # Add the default index on '_id'
    names.append('_id_')
    async for index in collection.list_indexes():
        if index['name'] not in names:
            log.warning(f"Bad index found in '{collection.name}', rebuilding")
            await collection.drop_indexes()
            break
