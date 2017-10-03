import time
import asyncio
import logging
from math import ceil
from uuid import uuid4
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError
from motor.motor_asyncio import AsyncIOMotorClient


log = logging.getLogger(__name__)


def timed(func):
    async def wraps(*args, **kwargs):
        start = time.time()
        r = await func(*args, **kwargs)
        log.debug("'%s' took %s s", func.__name__, time.time() - start)
        return r
    return wraps


class Migrator:

    """
    Used to fill a collection and progressively erase an old one.
    """

    def __init__(self, old_collection, new_collection):
        self._old_collection = old_collection
        self._new_collection = new_collection
        self._old_bulk = old_collection.initialize_unordered_bulk_op()
        self._new_bulk = new_collection.initialize_unordered_bulk_op()
        self._cursor = old_collection.find()
        self._batch_size = 0
        self.count = 0

    @property
    def old(self):
        return self._old_bulk

    @property
    def new(self):
        return self._new_bulk

    def __aiter__(self):
        return self

    async def __anext__(self):
        self._batch_size += 1
        self.count += 1

        # Insert in bulk. The batch size is a cpu/memory compromise.
        if self._batch_size == 200:
            await asyncio.wait([
                asyncio.ensure_future(self._old_bulk.execute()),
                asyncio.ensure_future(self._new_bulk.execute()),
            ])
            self._batch_size = 0
            self._old_bulk = self._old_collection.initialize_unordered_bulk_op()
            self._new_bulk = self._new_collection.initialize_unordered_bulk_op()

        # Return one document.
        await self._cursor.fetch_next
        doc = self._cursor.next_object()

        # End of iteration, insert the rest.
        if not doc:
            if self._batch_size > 0:
                await asyncio.wait([
                    asyncio.ensure_future(self._old_bulk.execute()),
                    asyncio.ensure_future(self._new_bulk.execute()),
                ])
            await self._old_collection.drop()
            raise StopAsyncIteration

        return doc


class Migration:

    def __init__(self, host, database, validate_on_start=None, **kwargs):
        client = AsyncIOMotorClient(host, **kwargs)
        self.db = client[database]

    @timed
    async def run(self):
        """
        Run the migrations.
        """
        log.info("Starting migrations on database '%s'", self.db.name)
        collections = await self.db.collection_names()
        if 'metadata' in collections:
            await self._migrate_workflow_metadata()
        if 'templates' in collections:
            await self._migrate_workflow_templates()
        if 'workflow-instances' in collections:
            await self._migrate_workflow_instances()
        if 'task-instances' in collections:
            await self._migrate_task_instances()
        if 'instances' in collections:
            await self._migrate_old_instances()
        log.info("Migration on database '%s' passed", self.db.name)

    @timed
    async def _migrate_workflow_metadata(self):
        old_col = self.db['metadata']
        new_col = self.db['workflow_metadata']
        migrator = Migrator(old_col, new_col)
        async for metadata in migrator:
            metadata['workflow_template_id'] = metadata.pop('id')
            migrator.new.insert(metadata)
            migrator.old.find({'_id': metadata['_id']}).remove_one()
        log.info('%s workflow metadata migrated', migrator.count)

    @timed
    async def _migrate_workflow_templates(self):
        """
        Replace documents with new structure.
        This uses a particular search filter because of the 'draft' attribute.
        """
        count = 0
        template_state = [None, None]
        sort = [('id', ASCENDING), ('version', DESCENDING)]
        wf_col = self.db['workflow_templates']
        task_col = self.db['task_templates']

        # Sorted on version number to set the proper 'state' values.
        async for template in self.db['templates'].find(None, sort=sort):
            count += 1
            # Ensure we got only one 'draft' if any, one 'active' if any,
            # and the rest 'archived'.
            if template['id'] != template_state[0]:
                template_state = [template['id'], 'active']
            if template['draft'] is True:
                state = 'draft'
            else:
                state = template_state[1]
                if state == 'active':
                    template_state[1] = 'archived'

            template['state'] = state
            template['timeout'] = None
            del template['draft']
            # Remove tasks.
            tasks = template.pop('tasks')
            await wf_col.replace_one(
                {'_id': template['_id']}, template, upsert=True
            )

            # Migrate and insert task templates.
            workflow_template = {
                'id': template['id'],
                'version': template['version'],
            }
            task_bulk = task_col.initialize_unordered_bulk_op()
            for task in tasks:
                task['workflow_template'] = workflow_template
                task = self._migrate_one_task_template(task)
                task_bulk.find({
                    'id': task['id'],
                    'workflow_template.id': workflow_template['id'],
                    'workflow_template.version': workflow_template['version'],
                }).upsert().replace_one(task)
            if tasks:
                await task_bulk.execute()

        await self.db['templates'].drop()
        log.info('%s workflow templates splited and migrated', count)

    @timed
    async def _migrate_workflow_instances(self):
        """
        Replace workflow instance documents with new structure.
        """
        old_col = self.db['workflow-instances']
        new_col = self.db['workflow_instances']
        migrator = Migrator(old_col, new_col)
        async for workflow in migrator:
            # Convert workflow instance.
            instance = workflow.pop('exec')
            instance['_id'] = workflow.pop('_id')
            instance['template'] = workflow
            # Insert.
            migrator.new.insert(instance)
            migrator.old.find({'_id': instance['_id']}).remove_one()
        log.info('%s workflow instances migrated', migrator.count)

    def _migrate_one_task_template(self, template):
        """
        Do the task configuration migrations.
        Add the new 'timeout' field.
        """
        config = template['config']
        template['timeout'] = None

        if template['name'] == 'join':
            template['timeout'] = config.pop('timeout', None)

        elif template['name'] == 'trigger_workflow':
            template['timeout'] = config.get('timeout')
            template['config'] = {
                'blocking': config.get('await_completion', True),
                'template': {
                    'service': 'twilio' if 'twilio' in config.get('nyuki_api', '') else 'pipeline',
                    'id': config.get('template', ''),
                    'draft': config.get('draft', False),
                },
            }

        elif template['name'] in ['send_sms', 'call', 'wait_sms', 'wait_email', 'wait_call']:
            if 'blocking' in config:
                template['timeout'] = config['blocking']['timeout']
                config['blocking'] = True

        elif template['name'] == 'gather':
            if 'finishOnKey' in config:
                config['finish_on_key'] = config.pop('finishOnKey')
            if 'numDigits' in config:
                config['num_digits'] = config.pop('numDigits')
            # Remove finish_on_key if num_digits is 1
            if 'finish_on_key' in config and config.get('num_digits') == 1:
                del config['finish_on_key']

        elif template['name'] == 'record':
            if 'finishOnKey' in config:
                config['finish_on_key'] = config.pop('finishOnKey')
            if 'maxLength' in config:
                config['max_length'] = config.pop('maxLength')
            if 'playBeep' in config:
                config['play_beep'] = config.pop('playBeep')

        return template

    def _new_task(self, task, workflow_instance_id=None):
        """
        Migrate a task instance.
        """
        # If task was never executed, fill it with 'not-started'.
        instance = task.pop('exec') or {
            'id': str(uuid4()),
            'status': 'not-started',
            'start': None,
            'end': None,
            'inputs': None,
            'outputs': None,
            'reporting': None,
        }
        if '_id' in task:
            instance['_id'] = task.pop('_id')
        instance['workflow_instance_id'] = workflow_instance_id or task.pop('workflow_exec_id')
        instance['template'] = self._migrate_one_task_template(task)

        if instance['reporting'] is None:
            return instance

        # Reporting migrations
        if instance['template']['name'] == 'group_selector':
            rgroups = instance['reporting'].get('groups', {})
            if isinstance(rgroups, dict):
                new_reporting = []
                for gid, medias in rgroups.items():
                    new_reporting.append({
                        'name': '',
                        'medias': medias,
                        'uid': gid,
                    })
                instance['reporting'] = {'groups': new_reporting}

        elif instance['template']['name'] == 'factory':
            instance['outputs']['diff'] = instance['reporting']
            instance['reporting'] = None

        if instance['reporting'] and 'contacts' in instance['reporting']:
            if isinstance(instance['reporting']['contacts'], dict):
                instance['reporting']['contacts'] = list(instance['reporting']['contacts'].values())

        return instance

    @timed
    async def _migrate_task_instances(self):
        """
        Replace documents with new structure.
        """
        old_col = self.db['task-instances']
        new_col = self.db['task_instances']
        migrator = Migrator(old_col, new_col)
        async for task in migrator:
            # Convert task instance.
            instance = self._new_task(task)
            # Insert.
            migrator.new.insert(instance)
            migrator.old.find({'_id': instance['_id']}).remove_one()
        log.info('%s task instances migrated', migrator.count)

    @timed
    async def _migrate_old_instances(self):
        """
        Bring back the old 'instances' collection from the dead.
        These documents are way more older than the others, so the migrations
        are a bit more complex and specific.
        """
        task_count = 0
        old_col = self.db['instances']
        workflow_col = self.db['workflow_instances']
        task_col = self.db['task_instances']
        migrator = Migrator(old_col, workflow_col)
        async for workflow in migrator:
            # Split tasks from their workflow instance.
            tasks = workflow.pop('tasks')
            # Convert workflow instance.
            instance = workflow.pop('exec')
            instance['_id'] = workflow.pop('_id')
            instance['template'] = workflow
            task_count += len(tasks)

            # Convert and insert task instances.
            for index, task in enumerate(tasks):
                if task['exec']:
                    # Set inputs and outputs as dicts (could be None).
                    for io in ('inputs', 'outputs'):
                        if not isinstance(task['exec'][io], dict):
                            task['exec'][io] = {}

                # Migrate from the old send_email task reporting/config.
                if task['name'] == 'send_email':
                    config = task['config']
                    task['config'] = {
                        'recipients': {'source': 'field', 'value': 'recipients'},
                        'subject': config['subject'],
                        'message': config['message'],
                        'default_placeholder': config.get('default_placeholder', ''),
                        'sender': {'address': config.get('sender')},
                    }

                    if task['exec'] and task['exec']['reporting'] is not None:
                        old_reporting = task['exec']['reporting']
                        contacts_len = len(old_reporting['contacts'])
                        reporting = {
                            'subject': old_reporting['subject'],
                            'message': old_reporting['message'],
                            'contacts': [],
                            'delivery': {
                                'expected': contacts_len,
                                'sent': 0,
                                'error': 0,
                            },
                        }
                        # Contacts
                        for contact in old_reporting['contacts'].values():
                            new_contact = {
                                'display_name': ' '.join([contact.pop('last_name', ''), contact.pop('first_name', '')]),
                                'external': False,
                                'email': contact.get('email'),
                            }
                            if contact['state'] == 'sent':
                                reporting['delivery']['sent'] += 1
                                new_contact['state'] = 'sent'
                            elif contact['state'] in ('pending', 'error'):
                                new_contact['state'] = 'error'
                                reporting['delivery']['error'] += 1
                            reporting['contacts'].append(new_contact)

                        task['exec']['reporting'] = reporting

                # Migrate from the old send_sms task reporting/config.
                elif task['name'] == 'send_sms':
                    config = task['config']
                    task['config'] = {
                        'recipients': {'source': 'field', 'value': 'recipients'},
                        'message': config['message'],
                        'encoding': config.get('encoding', 'utf-8'),
                        'max_sms': config.get('max_sms'),
                        'default_placeholder': config.get('default_placeholder', ''),
                    }

                    if task['exec'] and task['exec']['reporting'] is not None:
                        old_reporting = task['exec']['reporting']
                        contacts_len = len(old_reporting['contacts'])
                        reporting = {
                            'message': old_reporting['message'],
                            'contacts': [],
                            'delivery': {
                                'expected': contacts_len,
                                'sent': 0,
                                'error': 0,
                            },
                        }
                        # Contacts
                        for contact in old_reporting['contacts'].values():
                            new_contact = {
                                'display_name': ' '.join([contact.pop('last_name', ''), contact.pop('first_name', '')]),
                                'external': False,
                                'mobile': contact.get('mobile'),
                            }
                            if contact['state'] == 'started':
                                reporting['delivery']['sent'] += 1
                                new_contact['state'] = 'sent'
                            elif contact['state'] == 'done':
                                new_contact['state'] = 'delivered'
                            elif contact['state'] == 'error':
                                reporting['delivery']['error'] += 1
                                new_contact['state'] = 'error'
                            reporting['contacts'].append(new_contact)

                        task['exec']['reporting'] = reporting

                # Migrate from the old wait_input task reporting/config.
                elif task['name'] in ('wait_sms', 'wait_email', 'wait_call'):
                    config = task['config']
                    task['config'] = {
                        'emitters': {'source': 'field', 'value': 'emitters'},
                        'rules': config['rules'],
                    }
                    if 'timeout' in config:
                        # This field will go away shortly after.
                        task['config']['blocking'] = {'timeout': config['timeout']}
                    # 'success_condition' => 'quorum_policy'
                    if 'success_condition' in config:
                        task['config']['quorum_policy'] = {
                            'method': 'count' if config['success_condition']['type'] == 'number' else 'rate',
                            'value': config['success_condition']['value'],
                        }

                    if task['exec'] and task['exec']['reporting'] is not None and 'emitters' in task['exec']['reporting']:
                        old_reporting = task['exec']['reporting']
                        contacts_len = len(old_reporting['emitters'])
                        reporting = {
                            'contacts': [],
                            'inputs': {
                                'expected': contacts_len,
                                'progress': 0,
                                'received': {
                                    'positive': 0,
                                    'negative': 0,
                                    'unknown': 0,
                                },
                                'failed': 0
                            }
                        }
                        # Quorum
                        quorum_policy = task['config'].get('quorum_policy')
                        if quorum_policy:
                            reporting['quorum'] = {
                                'reached': False,
                                'expected': ceil(quorum_policy['value'] if quorum_policy['method'] == 'count' else (contacts_len * quorum_policy['value']) / 100),
                                'progress': len(old_reporting['inputs_accepted']),
                            }
                            reached = (reporting['quorum']['progress'] >= reporting['quorum']['expected'])
                            reporting['quorum']['reached'] = reached
                            task['exec']['outputs']['quorum'] = reached

                        # Contacts
                        positives = {contact['source']: contact['time'] for contact in old_reporting['inputs_accepted']}
                        unknowns = {contact['source']: contact['time'] for contact in old_reporting['inputs_invalid']}
                        for contact in old_reporting['emitters']:
                            new_contact = {
                                'uid': contact['uid'],
                                'external': False,
                                'display_name': ' '.join([contact.get('last_name', ''), contact.get('first_name', '')]),
                                'input': None,
                                'source': None,
                                'received_at': None,
                                'error': False,
                            }

                            result = None
                            if task['name'] == 'wait_sms':
                                source = contact.get('mobile')
                                if source in positives.keys():
                                    result = 'positive'
                                    received_at = positives[source]
                                elif source in unknowns:
                                    result = 'unknown'
                                    received_at = unknowns[source]
                            elif task['name'] == 'wait_email':
                                source = contact.get('email')
                                if source in positives:
                                    result = 'positive'
                                    received_at = positives[source]
                                elif source in unknowns:
                                    result = 'unknown'
                                    received_at = unknowns[source]
                            elif task['name'] == 'wait_call':
                                for media in ('mobile', 'phone_work', 'phone_home'):
                                    source = contact.get(media)
                                    if source in positives:
                                        result = 'positive'
                                        received_at = positives[source]
                                    elif source in unknowns:
                                        result = 'unknown'
                                        received_at = unknowns[source]
                                    break

                            if result is None:
                                result = 'unknown'
                                received_at = task['exec']['end']

                            new_contact['input'] = result
                            new_contact['received_at'] = received_at
                            reporting['inputs']['progress'] += 1
                            reporting['inputs']['received'][result] += 1
                            reporting['contacts'].append(new_contact)

                        task['exec']['reporting'] = reporting

                # Migrate from the old call task reporting/config.
                elif task['name'] == 'call':
                    config = task['config']
                    task['config'] = {
                        'recipients': {'source': 'field', 'value': 'recipients'},
                        'template': {'id': config['template'], 'draft': config.get('draft', False)},
                    }
                    if 'global_timeout' in config:
                        # This field will go away shortly after.
                        task['config']['blocking'] = {'timeout': config['global_timeout']}
                    # 'success_condition' => 'quorum_policy'
                    if 'success_condition' in config:
                        task['config']['quorum_policy'] = {
                            'method': 'count' if config['success_condition']['type'] == 'number' else 'rate',
                            'value': config['success_condition']['value'],
                        }
                    if task['exec'] and task['exec']['reporting'] is not None:
                        old_reporting = task['exec']['reporting']
                        contacts_len = len(old_reporting['contacts'])
                        reporting = {
                            'contacts': [],
                            'feedbacks': {
                                'expected': contacts_len,
                                'progress': contacts_len,
                                'received': {
                                    'positive': 0,
                                    'negative': 0,
                                    'unknown': 0,
                                },
                                'failed': 0
                            }
                        }
                        # Quorum
                        quorum_policy = task['config'].get('quorum_policy')
                        if quorum_policy:
                            reporting['quorum'] = {
                                'reached': False,
                                'expected': ceil(quorum_policy['value'] if quorum_policy['method'] == 'count' else (contacts_len * quorum_policy['value']) / 100),
                                'progress': old_reporting['promises']['count'],
                            }
                            reached = (reporting['quorum']['progress'] >= reporting['quorum']['expected'])
                            reporting['quorum']['reached'] = reached
                            task['exec']['outputs']['quorum'] = reached
                        # Contacts
                        for contact in old_reporting['contacts'].values():
                            contact['display_name'] = ' '.join([contact.pop('last_name', ''), contact.pop('first_name', '')])
                            contact['external'] = False
                            contact['attempts'] = 1
                            contact['feedback'] = 'unknown'
                            contact['calls'] = []
                            medias = contact.pop('medias', {})
                            for media, call in medias.items():
                                if call.get('promise') is True:
                                    contact['feedback'] = 'positive'
                                contact['calls'].append({
                                    'uid': str(uuid4())[:8],
                                    'media': media,
                                    'number': contact.pop(media, None),
                                    'attempt': 1,
                                    'feedback': 'positive' if call.get('promise') is True else 'unknown',
                                    'status': 'completed',
                                    'workflow': call.get('workflow_exec_id'),
                                    'start': call['start'],
                                    'end': call['end'] if 'end' in call else None,
                                })
                            contact.pop('email', None)
                            reporting['contacts'].append(contact)
                            reporting['feedbacks']['received'][contact['feedback']] += 1

                        task['exec']['reporting'] = reporting

                tasks[index] = self._new_task(task, instance['id'])

            try:
                await task_col.insert_many(tasks)
            except BulkWriteError:
                pass

            # Insert workflow instance.
            migrator.new.insert(instance)
            migrator.old.find({'_id': instance['_id']}).remove_one()

        log.info(
            '%s old instances migrated to new format (including %s tasks)',
            migrator.count, task_count,
        )


if __name__ == '__main__':
    logging.basicConfig(format='%(message)s', level='DEBUG')
    m = Migration('localhost', 'twilio')
    asyncio.get_event_loop().run_until_complete(m.run())
