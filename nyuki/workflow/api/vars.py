import re
import logging
from pymongo.errors import AutoReconnect

from nyuki.api import Response, resource, HTTPBreak


log = logging.getLogger(__name__)


class DataInspector(object):

    REGEX = [
        re.compile('{([a-zA-Z_\-]+)}', re.IGNORECASE),
        re.compile('@([a-zA-Z_\-]+)', re.IGNORECASE)
    ]

    @classmethod
    def iter(cls, node):
        if isinstance(node, dict):
            for value in node.values():
                yield from cls.iter(value)
        elif isinstance(node, list):
            for value in node:
                yield from cls.iter(value)
        else:
            yield node

    async def required_keys(self, tid, version=None, draft=False):
        try:
            template = await self.nyuki.storage.get_template(
                tid, draft=draft, version=version
            )
        except AutoReconnect:
            raise HTTPBreak(503)
        if not template:
            raise HTTPBreak(404, {'error': 'template not found'})

        keys = set()
        for task in template.get('tasks', []):
            for key, data in task.get('config', {}).items():
                # Get all evaluable inner-data
                for value in self.iter(data):
                    if not isinstance(value, str):
                        continue
                    for regex in self.REGEX:
                        for data_key in regex.findall(value):
                            keys.add(data_key)
        return list(keys)


@resource('/workflow/vars/{tid}', versions=['v1'])
class ApiVars(DataInspector):

    async def get(self, request, tid):
        keys = await self.required_keys(tid)
        return Response(body=keys)


@resource('/workflow/vars/{tid}/{version:\d+}', versions=['v1'])
class ApiVarsVersion(DataInspector):

    async def get(self, request, tid, version):
        keys = await self.required_keys(tid, version=version)
        return Response(body=keys)


@resource('/workflow/vars/{tid}/draft', versions=['v1'])
class ApiVarsDraft(DataInspector):

    async def get(self, request, tid):
        keys = await self.required_keys(tid, draft=True)
        return Response(body=keys)
