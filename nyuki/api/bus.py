import asyncio

from nyuki.utils import from_isoformat

from .api import Response, resource


@resource('/bus/topics', versions=['v1'])
class ApiBusTopics:

    async def get(self, request):
        try:
            self.nyuki._services.get('bus')
        except KeyError:
            return Response(status=404)
        return Response(self.nyuki.bus.topics)


@resource('/bus/publish', versions=['v1'])
class ApiBusPublish:

    async def post(self, request):
        try:
            self.nyuki._services.get('bus')
        except KeyError:
            return Response(status=404)
        request = await request.json()
        asyncio.ensure_future(self.nyuki.bus.publish(
            request.get('data', {}),
            request.get('topic', 'testing'),
            request.get('qos', 0),
        ))
