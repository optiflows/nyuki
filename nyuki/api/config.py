import asyncio
import json
from jsonschema import ValidationError
import logging

from .api import Response, resource


log = logging.getLogger(__name__)


@resource('/config', versions=['v1'])
class ApiConfiguration:

    def get(self, request):
        return Response(self.nyuki._config)

    async def patch(self, request):
        body = await request.json()

        try:
            self.nyuki.update_config(body)
        except ValidationError as error:
            error = {'error': error.message}
            log.error('Bad configuration received : {}'.format(body))
            log.debug(error)
            return Response(body=error, status=400)

        # Reload what is necessary, return the http response immediately
        self.nyuki.save_config()
        asyncio.ensure_future(self.nyuki._reload_config(body))

        return Response(self.nyuki._config)
