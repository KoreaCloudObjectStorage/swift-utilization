import calendar
import time
from datetime import datetime

__all__ = ['version_info', 'version']

#: Version information ``(major, minor, revision)``.
version_info = (0, 0, 1)
#: Version string ``'major.minor.revision'``.
version = '.'.join(map(str, version_info))


def iso8601_to_timestamp(strtime):
        return calendar.timegm(datetime.strptime(strtime, "%Y-%m-%dT%H:%M:%S")
                               .timetuple())


def timestamp_to_iso8601(timestamp):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(timestamp))