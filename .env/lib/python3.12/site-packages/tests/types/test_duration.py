# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import datetime
import isodate
from tableschema import types
from tableschema.config import ERROR


# Tests

@pytest.mark.parametrize('format, value, result', [
    ('default', isodate.Duration(years=1), isodate.Duration(years=1)),
    ('default', 'P1Y10M3DT5H11M7S',
         isodate.Duration(years=1, months=10, days=3, hours=5, minutes=11, seconds=7)),
    ('default', 'P1Y', isodate.Duration(years=1)),
    ('default', 'P1M', isodate.Duration(months=1)),
    ('default', 'PT1S', datetime.timedelta(seconds=1)),
    ('default', datetime.timedelta(seconds=1),  datetime.timedelta(seconds=1)),
    ('default', 'P1M1Y', ERROR),
    ('default', 'P-1Y', ERROR),
    ('default', 'year', ERROR),
    ('default', True, ERROR),
    ('default', False, ERROR),
    ('default', 1, ERROR),
    ('default', '', ERROR),
    ('default', [], ERROR),
    ('default', {}, ERROR),
])
def test_cast_duration(format, value, result):
    assert types.cast_duration(format, value) == result
