from datetime import datetime, timezone


def from_isoformat(iso):
    return datetime.strptime(iso, '%Y-%m-%dT%H:%M:%S.%f')


def utcnow():
    return datetime.now(timezone.utc)
