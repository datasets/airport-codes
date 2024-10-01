# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
import warnings
from datetime import datetime
from dateutil.parser import parse
from ..config import ERROR


# Module API

def cast_datetime(format, value, **options):
    if not isinstance(value, datetime):
        if not isinstance(value, six.string_types):
            return ERROR
        try:
            if format == 'default':
                value = datetime.strptime(value, _DEFAULT_PATTERN)
            elif format == 'any':
                value = parse(value)
            else:
                if format.startswith('fmt:'):
                    warnings.warn(
                        'Format "fmt:<PATTERN>" is deprecated. '
                        'Please use "<PATTERN>" without "fmt:" prefix.',
                        UserWarning)
                    format = format.replace('fmt:', '')
                value = datetime.strptime(value, format)
        except Exception:
            return ERROR
    return value


# Internal

_DEFAULT_PATTERN = '%Y-%m-%dT%H:%M:%SZ'
