from aiohttp import web
from aiohttp.hdrs import METH_ALL
import asyncio
from functools import partial
import json
import logging

from nyuki.services import Service
from nyuki.utils import serialize_object


log = logging.getLogger(__name__)
# aiohttp needs its own logger, and always prints HTTP hits using INFO level
access_log = logging.getLogger('.'.join([__name__, 'access']))
access_log.info = access_log.debug


def resource(path, versions=None, content_type='application/json'):
    """
    Nyuki resource decorator to register a route.
    A resource has multiple HTTP methods (get, post, etc).
    """
    def decorated(cls):
        cls.RESOURCE_CLASS = ResourceClass(cls, path, versions, content_type)
        return cls
    return decorated


def content_type(content_type):
    """
    Decorator to set a specific content_type to one method (get, post...)
    """
    def decorated(func):
        func.CONTENT_TYPE = content_type
        return func
    return decorated


class HTTPBreak(Exception):

    def __init__(self, status, body=None):
        self.status = status
        self.body = body


class Response(web.Response):

    """
    Overrides aiohttp's response to facilitate its usage.
    """

    def __init__(self, body=None, **kwargs):

        # Check json
        if isinstance(body, dict) or isinstance(body, list):
            body = json.dumps(body, default=serialize_object).encode()
            if not self._get_content_type(kwargs):
                kwargs['content_type'] = 'application/json'

        return super().__init__(body=body, **kwargs)

    def _get_content_type(self, kwargs):
        return kwargs.get('content_type') or kwargs.get('headers', {}).get('Content-Type')


async def mw_capability(app, capa_handler):
    """
    Transform the request data to be passed through a capability and
    convert the result into a web response.
    """
    POST_METHODS = web.Request.POST_METHODS - {'DELETE'}

    async def middleware(request):
        # Ensure a content-type check is necessary
        # aiohttp includes DELETE in post methods, we don't want that
        if request.method in POST_METHODS and getattr(capa_handler, 'CONTENT_TYPE', None):
            ctype = capa_handler.CONTENT_TYPE
            # Check content_type from @resource or @content_type decorators
            request_types = request.headers.get('Content-Type', '').split(';')
            required_types = ctype.split(';')
            for required in required_types:
                if required not in request_types:
                    log.debug(
                        "content-type '%s' required. Received '%s'",
                        required, request_types
                    )
                    return Response({'error': 'Wrong or Missing content-type'}, status=400)

            # Check application/json is really a JSON body
            if 'application/json' in required_types:
                try:
                    await request.json()
                except json.decoder.JSONDecodeError:
                    log.debug('request body for application/json must be JSON')
                    return Response(
                        {'error': 'application/json requires a JSON body'},
                        status=400
                    )

            # Check multipart/form-data is really a post form
            if 'multipart/form-data' in required_types:
                try:
                    await request.post()
                except ValueError as exc:
                    log.debug(exc)
                    return Response(status=400, body={
                        'error': 'multipart/form-data must be a form'
                    })

        try:
            capa_resp = await capa_handler(request, **request.match_info)
        except (web.HTTPNotFound, web.HTTPMethodNotAllowed):
            # Avoid sending a report on a simple 404/405
            raise
        except HTTPBreak as exc:
            return Response(exc.body, status=exc.status)

        if isinstance(capa_resp, Response):
            return capa_resp
        return Response()

    return middleware


class ResourceClass:

    """
    Allow the extensivity of the nyuki's HTTP resources using the webserver's
    router from the `Api` class.
    """

    def __init__(self, cls, path, versions, content_type):
        self.cls = cls
        self.path = path
        self.versions = versions
        self.content_type = content_type

    def _add_routes(self, router, path):
        resource = router.add_resource(path)
        cls_instance = self.cls()
        for method in METH_ALL:
            handler = getattr(self.cls, method.lower(), None)
            if handler is not None:
                async_handler = asyncio.coroutine(partial(handler, cls_instance))
                async_handler.CONTENT_TYPE = getattr(
                    handler, 'CONTENT_TYPE', self.content_type
                )
                route = resource.add_route(method, async_handler)
                log.debug('Added route: %s', route)

    def register(self, nyuki, router):
        self.cls.nyuki = nyuki
        if not self.versions:
            self._add_routes(router, self.path)
        else:
            for version in self.versions:
                route = '/{}{}'.format(version, self.path)
                self._add_routes(router, route)


class Api(Service):

    """
    Manage a webserver built using the nyuki's defined HTTP resources
    """

    CONF_SCHEMA = {
        "type": "object",
        "required": ["api"],
        "properties": {
            "api": {
                "type": "object",
                "properties": {
                    "host": {"type": "string"},
                    "port": {"type": "integer"}
                }
            }
        }
    }

    def __init__(self, nyuki):
        self._nyuki = nyuki
        self._nyuki.register_schema(self.CONF_SCHEMA)
        self._loop = self._nyuki.loop or asyncio.get_event_loop()
        self._host = None
        self._port = None
        self._middlewares = [mw_capability]
        self._app = None
        self._handler = None
        self._server = None

    @property
    def capabilities(self):
        return self._nyuki.HTTP_RESOURCES

    def configure(self, host='0.0.0.0', port=5558):
        self._host = host
        self._port = port

    async def start(self):
        """
        Expose capabilities by building the HTTP server.
        The server will be started with the event loop.
        """
        self._app = web.Application(
            loop=self._loop, middlewares=self._middlewares
        )
        for resource in self._nyuki.HTTP_RESOURCES:
            resource.RESOURCE_CLASS.register(self._nyuki, self._app.router)
        log.info("Starting the http server on {}:{}".format(self._host, self._port))
        self._handler = self._app.make_handler(access_log=access_log)
        self._server = await self._loop.create_server(
            self._handler, host=self._host, port=self._port
        )

    async def stop(self):
        log.info('Stopped the http server')
        self._server.close()
        await self._handler.shutdown()
        await self._server.wait_closed()
        self._app = None
        self._handler = None
        self._server = None
