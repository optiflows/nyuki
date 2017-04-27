from functools import singledispatch
from datetime import datetime, tzinfo, timedelta


class SimpleUTC(tzinfo):

    def tzname(self):
        return 'UTC'

    def utcoffset(self, dt):
        return timedelta(0)


@singledispatch
def serialize_object(obj):
    """
    Default serializer for python objects.
    """
    return 'Internal server data: {}'.format(type(obj))


@serialize_object.register(datetime)
def _serialize_datetime(dt):
    """
    Datetime serializer.
    """
    return dt.replace(tzinfo=SimpleUTC()).isoformat()
