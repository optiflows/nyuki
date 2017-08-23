from tukio.task.join import JoinTask

from .factory import FactoryTask, FACTORY_SCHEMAS
from .python_script import PythonScript
from .report import ReportTask
from .sleep import SleepTask
from .task_selector import TaskSelector
from .trigger_workflow import TriggerWorkflowTask


# Generic schema to reference a task ID
TASKID_SCHEMA = {
    'type': 'string',
    'description': 'task_id',
    'minLength': 1,
    'maxLength': 128
}


# Add a schema to the join task
JoinTask.SCHEMA = {
    'type': 'object',
    'required': ['wait_for'],
    'properties': {
        'wait_for': {
            'anyOf': [
                {
                    'type': 'array',
                    'minItems': 2,
                    'maxItems': 64,
                    'uniqueItems': True,
                    'items': TASKID_SCHEMA
                },
                {'type': 'integer', 'minimum': 2}
            ]
        },
    }
}
