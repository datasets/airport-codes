# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
import json
from ..config import ERROR


# Module API

def cast_object(format, value, **options):
    if not isinstance(value, dict):
        if not isinstance(value, six.string_types):
            return ERROR
        try:
            value = json.loads(value)
        except Exception:
            return ERROR
        if not isinstance(value, dict):
            return ERROR
    return value
