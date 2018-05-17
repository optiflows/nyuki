from aiohttp import web
from asynctest import TestCase, Mock, patch, ignore_loop
from json import loads
from nose.tools import (
    assert_is, assert_is_not_none, assert_raises, assert_true, eq_
)

from nyuki.api.api import Api, mw_capability, Response

from tests import make_future


class TestCapabilityMiddleware(TestCase):

    def setUp(self):
        self._request = Mock()
        self._request.POST_METHODS = ['POST', 'PUT', 'PATCH']
        self._app = Mock()
        async def json():
            return {'capability': 'test'}
        async def text():
            return '{"capability": "test"}'
        self._request.json = json
        self._request.text = text

    async def test_001a_extract_data_from_payload_post_method(self):
        self._request.method = 'POST'
        self._request.match_info = {'name': 'test'}

        async def _capa_handler(d, name):
            eq_(name, 'test')
            capa_resp = Response({'response': 'ok'})
            return capa_resp

        mdw = await mw_capability(self._app, _capa_handler)
        assert_is_not_none(mdw)
        response = await mdw(self._request)
        assert_true(isinstance(response, web.Response))
        eq_(loads(response.body.decode('utf-8'))["response"], 'ok')
        eq_(response.status, 200)

    async def test_001b_extract_data_from_non_post_method(self):
        self._request.method = 'GET'
        self._request.query = {'id': 2}
        self._request.match_info = {'name': 'test'}

        async def _capa_handler(d, name):
            eq_(name, 'test')
            capa_resp = Response({'response': 2})
            return capa_resp

        mdw = await mw_capability(self._app, _capa_handler)
        assert_is_not_none(mdw)
        response = await mdw(self._request)
        assert_true(isinstance(response, web.Response))
        eq_(loads(response.body.decode('utf-8'))["response"], 2)
        eq_(response.status, 200)

    async def test_001c_post_no_data(self):
        self._request.method = 'POST'
        self._request.match_info = {'name': 'test'}

        async def _capa_handler(d, name):
            eq_(name, 'test')
            capa_resp = Response({'response': 'ok'})
            return capa_resp

        mdw = await mw_capability(self._app, _capa_handler)
        assert_is_not_none(mdw)
        response = await mdw(self._request)
        assert_true(isinstance(response, web.Response))
        eq_(loads(response.body.decode('utf-8'))["response"], 'ok')
        eq_(response.status, 200)

    async def test_002_no_response(self):
        self._request.method = 'GET'
        self._request.query = {}
        self._request.match_info = {}

        async def _capa_handler(d):
            return Response()

        mdw = await mw_capability(self._app, _capa_handler)
        assert_is_not_none(mdw)
        response = await mdw(self._request)
        assert_true(isinstance(response, web.Response))
        eq_(response.body, None)
        eq_(response.status, 200)

    async def test_003_request_headers(self):
        self._request.method = 'POST'
        self._request.match_info = {'name': 'test'}
        self._request.headers = {'Content-Type': 'application/json'}

        ar = await self._request.json()
        eq_(ar['capability'], 'test')
        eq_(self._request.headers.get('Content-Type'), 'application/json')
