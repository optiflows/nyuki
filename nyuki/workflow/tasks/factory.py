import asyncio
import logging
from aiohttp import ClientSession
from copy import deepcopy
from tukio.task import register
from tukio.task.holder import TaskHolder

from nyuki.utils import Converter
from nyuki.utils.transform import Arithmetic
from nyuki.workflow.tasks.utils import runtime, generate_factory_schema


log = logging.getLogger(__name__)


FACTORY_SCHEMAS = {
    'extract': {
        'type': 'object',
        'required': ['type', 'fieldname', 'regex_id'],
        'properties': {
            'type': {'type': 'string', 'enum': ['extract']},
            'fieldname': {'type': 'string', 'minLength': 1},
            'regex_id': {'type': 'string', 'minLength': 1},
            'pos': {'type': 'integer', 'minimum': 0},
            'endpos': {'type': 'integer', 'minimum': 0},
            'flags': {'type': 'integer'}
        }
    },
    'lookup': {
        'type': 'object',
        'required': ['type', 'fieldname', 'lookup_id'],
        'properties': {
            'type': {'type': 'string', 'enum': ['lookup']},
            'fieldname': {'type': 'string', 'minLength': 1},
            'lookup_id': {'type': 'string', 'minLength': 1},
            'icase': {'type': 'boolean'}
        }
    },
    'set': {
        'type': 'object',
        'required': ['type', 'fieldname', 'value'],
        'properties': {
            'type': {'type': 'string', 'enum': ['set']},
            'fieldname': {'type': 'string', 'minLength': 1},
            'value': {'description': 'any value'}
        }
    },
    'copy': {
        'type': 'object',
        'required': ['type', 'fieldname', 'copy'],
        'properties': {
            'type': {'type': 'string', 'enum': ['copy']},
            'fieldname': {'type': 'string', 'minLength': 1},
            # Name of the new field
            'copy': {'type': 'string', 'minLength': 1},
        }
    },
    'sub': {
        'type': 'object',
        'required': ['type', 'fieldname', 'regex_id', 'repl'],
        'properties': {
            'type': {'type': 'string', 'enum': ['sub']},
            'fieldname': {'type': 'string', 'minLength': 1},
            'regex_id': {'type': 'string', 'minLength': 1},
            'repl': {'type': 'string', 'minLength': 1},
            'count': {'type': 'integer', 'minimum': 1},
            'flags': {'type': 'integer'}
        }
    },
    'unset': {
        'type': 'object',
        'required': ['type', 'fieldname'],
        'properties': {
            'type': {'type': 'string', 'enum': ['unset']},
            'fieldname': {'type': 'string', 'minLength': 1}
        }
    },
    'arithmetic': {
        'type': 'object',
        'required': ['type', 'fieldname', 'operator', 'operand1', 'operand2'],
        'properties': {
            'type': {'type': 'string', 'enum': ['arithmetic']},
            'fieldname': {'type': 'string', 'minLength': 1},
            'operator': {
                'type': 'string',
                'enum': list(Arithmetic.OPS.keys())
            },
            'operand1': {'oneOf': [
                {'type': 'string', 'minLength': 1},
                {'type': 'number'},
            ]},
            'operand2': {'oneOf': [
                {'type': 'string', 'minLength': 1},
                {'type': 'number'},
            ]}
        }
    },
    'union': {
        'type': 'object',
        'required': ['type', 'fieldname', 'operand1', 'operand2'],
        'properties': {
            'type': {'type': 'string', 'enum': ['union']},
            'fieldname': {'type': 'string', 'minLength': 1},
            'operand1': {'oneOf': [
                {'type': 'string', 'minLength': 1},
                {'type': 'array'},
                {'type': 'object'},
            ]},
            'operand2': {'oneOf': [
                {'type': 'string', 'minLength': 1},
                {'type': 'array'},
                {'type': 'object'},
            ]}
        }
    },
}


@register('factory', 'execute')
class FactoryTask(TaskHolder):

    __slots__ = ('api_url', 'session')

    SCHEMA = generate_factory_schema(**FACTORY_SCHEMAS)

    def __init__(self, config):
        super().__init__(config)
        self.api_url = 'http://localhost:{}/v1/workflow'.format(
            runtime.config['api']['port']
        )
        self.session = None

    async def get_regex(self, rule):
        """
        Query the nyuki to get the actual regexes from their IDs
        """
        url = '{}/regexes/{}'.format(self.api_url, rule['regex_id'])
        async with self.session.get(url) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    'Could not find regex with id {}'.format(
                        rule['regex_id']
                    )
                )
            data = await resp.json()
            rule['pattern'] = data['pattern']
            del rule['regex_id']

    async def get_lookup(self, rule):
        """
        Query the nyuki to get the actual lookup tables from their IDs
        """
        url = '{}/lookups/{}'.format(self.api_url, rule['lookup_id'])
        async with self.session.get(url) as resp:
            if resp.status != 200:
                raise RuntimeError(
                    'Could not find lookup table with id {}'.format(
                        rule['lookup_id']
                    )
                )
            data = await resp.json()
            rule['table'] = {
                field['value']: field['replace']
                for field in data['table']
            }
            del rule['lookup_id']

    async def get_factory_rules(self, config):
        """
        Iterate through the task's configuration to swap from their IDs to
        their database equivalent within the nyuki
        """
        for rule in config['rules']:
            if rule['type'] in ['extract', 'sub']:
                await self.get_regex(rule)
            elif rule['type'] == 'lookup':
                await self.get_lookup(rule)
            elif rule['type'] == 'condition-block':
                for condition in rule['conditions']:
                    await self.get_factory_rules(condition)

    async def execute(self, event):
        data = event.data
        runtime_config = deepcopy(self.config)
        async with ClientSession() as self.session:
            await self.get_factory_rules(runtime_config)
        log.debug('Full factory config: %s', runtime_config)

        converter = Converter.from_dict(runtime_config)
        data['diff'] = converter.apply(data)
        log.debug('Conversion diff: %s', data['diff'])
        return data
