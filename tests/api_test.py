from aiohttp import errors, web
import asyncio
from asynctest import TestCase, Mock, patch, ignore_loop
from json import loads
from nose.tools import (
    assert_is, assert_is_not_none, assert_raises, assert_true, eq_
)

from nyuki.api import Api, mw_capability, mw_json, APIRequest
from nyuki.capabilities import Response
from tests import future_func, make_future


class TestApi(TestCase):

    def setUp(self):
        self._api = Api(self.loop)
        self._host = 'localhost'
        self._port = 8080

    @patch('aiohttp.web.Application.make_handler')
    @patch('asyncio.unix_events._UnixSelectorEventLoop.create_server')
    async def test_001_build_server(self, create_server_mock, handler_mock):
        await self._api.build(self._host, self._port)
        eq_(handler_mock.call_count, 1)
        eq_(create_server_mock.call_count, 1)
        create_server_mock.assert_called_with(
            self._api._handler, host=self._host, port=self._port
        )

    async def test_002_destroy_server(self):
        with patch.object(self._api, '_handler') as i_handler:
            i_handler.finish_connections.return_value = make_future([])
            with patch.object(self._api, '_server') as i_server:
                i_server.wait_closed.return_value = make_future([])
                with patch.object(self._api._server, 'close') as call_close:
                    await self._api.destroy()
                    eq_(call_close.call_count, 1)
                    eq_(i_server.wait_closed.call_count, 1)

    @ignore_loop
    def test_003_instantiated_router(self):
        assert_is(self._api.router, self._api._app.router)


class TestJsonMiddleware(TestCase):

    def setUp(self):
        self._request = Mock()
        self._request.POST_METHODS = ['POST', 'PUT', 'PATCH']
        self._app = Mock()

    async def test_001_request_valid_header_content_post_method(self):
        self._request.headers = {'CONTENT-TYPE': 'application/json'}
        ret_value = {'test_value': 'kikoo_test'}
        self._request.json.return_value = make_future(ret_value)
        self._request.method = 'POST'

        @future_func
        def _next_handler(r):
            return ret_value

        mdw = await mw_json(self._app, _next_handler)
        assert_is_not_none(mdw)
        response = await mdw(self._request)
        eq_(response, ret_value)

    async def test_002_request_handling_non_post_method(self):
        self._request.headers = {}
        ret_value = True
        self._request.method = 'GET'

        @future_func
        def _next_handler(r):
            return ret_value

        mdw = await mw_json(self._app, _next_handler)
        assert_is_not_none(mdw)
        response = await mdw(self._request)
        eq_(response, ret_value)

    async def test_003_request_invalid_or_missing_header(self):
        headers = [{}, {'CONTENT-TYPE': 'application/octet-stream'}]
        values = ['', 'this_is_octet']
        self._request.method = 'POST'

        @future_func
        def _next_handler(r):
            return 'dummy'

        for idx, h in enumerate(headers):
            self._request.headers = h
            self._request.content = values[idx]
            mdw = await mw_json(self._app, _next_handler)
            assert_is_not_none(mdw)
            if idx > 0:
                with assert_raises(errors.HttpBadRequest):
                    await mdw(self._request)


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

        @future_func
        def json():
            return {'capability': 'string_manip'}

        self._request.json = json

        @future_func
        def _capa_handler(d, name):
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
        self._request.GET = {'id': 2}
        self._request.match_info = {'name': 'test'}

        @future_func
        def _capa_handler(d, name):
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

        @future_func
        def _capa_handler(d, name):
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
        self._request.GET = {}
        self._request.match_info = {}

        @future_func
        def _capa_handler(d):
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

        ar = await APIRequest.from_request(self._request)
        eq_(ar['capability'], 'test')
        eq_(ar.headers.get('Content-Type'), 'application/json')
