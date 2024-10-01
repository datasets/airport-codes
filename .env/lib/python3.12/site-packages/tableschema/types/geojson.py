# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
import json
from ..config import ERROR
from ..profile import Profile


# Module API

def cast_geojson(format, value, **options):
    if isinstance(value, six.string_types):
        try:
            value = json.loads(value)
        except Exception:
            return ERROR
    if not isinstance(value, dict):
        return ERROR
    if format == 'default':
        try:
            _profile.validate(value)
        except Exception:
            return ERROR
    elif format == 'topojson':
        pass  # Accept any dict as possibly topojson for now
    return value


# Internal

_profile = Profile('geojson')
