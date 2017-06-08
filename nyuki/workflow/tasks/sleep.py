import asyncio
import logging
from tukio.task import register
from tukio.task.holder import TaskHolder


log = logging.getLogger(__name__)


@register('sleep', 'execute')
class SleepTask(TaskHolder):

    """
    Mainly a testing task
    """

    SCHEMA = {
        'type': 'object',
        'properties': {
            'time': {'type': 'integer', 'minimum': 1}
        }
    }

    async def execute(self, event):
        task = asyncio.Task.current_task()
        time = self.config.get('time', 2)
        log.info('%s: sleeping %s second%s', task.uid, time, 's' if time > 1 else '')
        await asyncio.sleep(time)
        log.info('%s: done sleeping', task.uid)
        return event.data
