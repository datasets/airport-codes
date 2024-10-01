# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
from typing import NamedTuple
from ..config import ERROR


# Module API

def cast_yearmonth(format, value, **options):
    if isinstance(value, (tuple, list)):
        if len(value) != 2:
            return ERROR
        value = _yearmonth(value[0], value[1])
    elif isinstance(value, six.string_types):
        try:
            year, month = value.split('-')
            year = int(year)
            month = int(month)
            if month < 1 or month > 12:
                return ERROR
            value = _yearmonth(year, month)
        except Exception:
            return ERROR
    else:
        return ERROR
    return value


# Internal
class _yearmonth(NamedTuple):
    year: int
    month: int
