class PersistenceBackend(object):

    """
    Base backend object for persistence, a persistence backend should
    overrides the required methods (store, retrieve).
    """

    async def init(self):
        pass

    async def store(self, event):
        raise NotImplementedError

    async def update(self, uid, status):
        raise NotImplementedError

    async def retrieve(self, since, status):
        raise NotImplementedError
