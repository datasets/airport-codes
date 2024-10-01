# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tableschema import types
from tableschema.config import ERROR


# Tests

@pytest.mark.parametrize('format, value, result', [
    ('default', (180, 90), (180, 90)),
    ('default', [180, 90], (180, 90)),
    ('default', '180,90', (180, 90)),
    ('default', '180, -90', (180, -90)),
    ('default', {'lon': 180, 'lat': 90}, ERROR),
    ('default', '181,90', ERROR),
    ('default', '0,91', ERROR),
    ('default', 'string', ERROR),
    ('default', 1, ERROR),
    ('default', '3.14', ERROR),
    ('default', '', ERROR),
    ('array', (180, 90), (180, 90)),
    ('array', [180, 90], (180, 90)),
    ('array', '[180, -90]', (180, -90)),
    #  ('array', {'lon': 180, 'lat': 90}, ERROR),
    ('array', [181, 90], ERROR),
    ('array', [0, 91], ERROR),
    ('array', '180,90', ERROR),
    ('array', 'string', ERROR),
    ('array', 1, ERROR),
    ('array', '3.14', ERROR),
    ('array', '', ERROR),
    #  ('object', {'lon': 180, 'lat': 90}, (180, 90)),
    ('object', '{"lon": 180, "lat": 90}', (180, 90)),
    ('object', '[180, -90]', ERROR),
    ('object', {'lon': 181, 'lat': 90}, ERROR),
    ('object', {'lon': 180, 'lat': -91}, ERROR),
    #  ('object', [180, -90], ERROR),
    ('object', '180,90', ERROR),
    ('object', 'string', ERROR),
    ('object', 1, ERROR),
    ('object', '3.14', ERROR),
    ('object', '', ERROR),
])
def test_cast_geopoint(format, value, result):
    assert types.cast_geopoint(format, value) == result
