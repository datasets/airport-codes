# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
import datetime
import isodate
from ..config import ERROR


# Module API

def cast_duration(format, value, **options):
    if not isinstance(value, (isodate.Duration, datetime.timedelta)):
        if not isinstance(value, six.string_types):
            return ERROR
        try:
            value = isodate.parse_duration(value)
        except Exception:
            return ERROR
    return value
