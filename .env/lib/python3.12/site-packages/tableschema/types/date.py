# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
import warnings
from datetime import datetime, date
from dateutil.parser import parse
from ..config import ERROR


# Module API

def cast_date(format, value, **options):
    if isinstance(value, datetime):
        value_time = value.time()
        if value_time.hour == 0 and value_time.minute == 0 and value_time.second == 0:
            return datetime(value.year, value.month, value.day).date()
        else:
            return ERROR

    if isinstance(value, date):
        return value

    if not isinstance(value, six.string_types):
        return ERROR

    # Parse string date
    try:
        if format == 'default':
            value = datetime.strptime(value, _DEFAULT_PATTERN).date()
        elif format == 'any':
            value = parse(value).date()
        else:
            if format.startswith('fmt:'):
                warnings.warn(
                    'Format "fmt:<PATTERN>" is deprecated. '
                    'Please use "<PATTERN>" without "fmt:" prefix.',
                    UserWarning)
                format = format.replace('fmt:', '')
            value = datetime.strptime(value, format).date()
    except Exception:
        return ERROR

    return value


# Internal

_DEFAULT_PATTERN = '%Y-%m-%d'
