# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
import json
from typing import NamedTuple
from decimal import Decimal
from ..config import ERROR


# Module API

def cast_geopoint(format, value, **options):

    # Parse
    if isinstance(value, six.string_types):
        try:
            if format == 'default':
                lon, lat = value.split(',')
                lon = lon.strip()
                lat = lat.strip()
            elif format == 'array':
                lon, lat = json.loads(value)
            elif format == 'object':
                if isinstance(value, six.string_types):
                    value = json.loads(value)
                if len(value) != 2:
                    return ERROR
                lon = value['lon']
                lat = value['lat']
            value = _geopoint(Decimal(lon), Decimal(lat))
        except Exception:
            return ERROR

    # Validate
    try:
        value = _geopoint(*value)
        if value.lon > 180 or value.lon < -180:
            return ERROR
        if value.lat > 90 or value.lat < -90:
            return ERROR
    except Exception:
        return ERROR

    return value


# Internal

class _geopoint(NamedTuple):
    lon: Decimal
    lat: Decimal

    def __repr__(self):
        return '[%s, %s]' % (self.lon, self.lat)
