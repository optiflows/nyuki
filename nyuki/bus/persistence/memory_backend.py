import logging

from nyuki.bus.persistence.events import EventStatus
from nyuki.bus.persistence.backend import PersistenceBackend


log = logging.getLogger(__name__)


class FIFOSizedQueue(object):

    def __init__(self, size):
        self._list = list()
        self._size = size

    def __len__(self):
        return len(self._list)

    @property
    def size(self):
        return self._size

    @property
    def list(self):
        return self._list

    @property
    def is_full(self):
        return len(self._list) >= self._size

    def put(self, item):
        while self.is_full:
            log.debug('queue full (%d), poping first item', len(self._list))
            self._list.pop(0)
        self._list.append(item)

    def empty(self):
        while self._list:
            yield self._list.pop(0)


class MemoryBackend(PersistenceBackend):

    def __init__(self, max_size=10000, **kwargs):
        self._last_events = FIFOSizedQueue(max_size)

    def __repr__(self):
        return '<MemoryBackend max_size={}>'.format(self._last_events.size)

    async def store(self, event):
        self._last_events.put(event)

    async def update(self, uid, status):
        for event in self._last_events.list:
            if event['id'] == uid:
                event['status'] = status.value
                return

    async def retrieve(self, since, status):
        def check_params(item):
            since_check = True
            status_check = True

            if since:
                since_check = item['created_at'] >= since

            if status:
                if isinstance(status, list):
                    status_check = EventStatus[item['status']] in status
                else:
                    status_check = item['status'] == status.value

            return since_check and status_check
        return list(filter(check_params, self._last_events.list))
