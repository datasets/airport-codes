# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import warnings
import pytest
from datetime import time
from tableschema import types
from tableschema.config import ERROR


# Tests

@pytest.mark.parametrize('format, value, result', [
    ('default', time(6), time(6)),
    ('default', '06:00:00', time(6)),
    ('default', '09:00', ERROR),
    ('default', '3 am', ERROR),
    ('default', '3.00', ERROR),
    ('default', 'invalid', ERROR),
    ('default', True, ERROR),
    ('default', '', ERROR),
    ('any', time(6), time(6)),
    ('any', '06:00:00', time(6)),
    ('any', '3:00 am', time(3)),
    ('any', 'some night', ERROR),
    ('any', 'invalid', ERROR),
    ('any', True, ERROR),
    ('any', '', ERROR),
    ('%H:%M', time(6), time(6)),
    ('%H:%M', '06:00', time(6)),
    ('%M:%H', '06:50', ERROR),
    ('%H:%M', '3:00 am', ERROR),
    ('%H:%M', 'some night', ERROR),
    ('%H:%M', 'invalid', ERROR),
    ('%H:%M', True, ERROR),
    ('%H:%M', '', ERROR),
    ('invalid', '', ERROR),
    # Deprecated
    ('fmt:%H:%M', time(6), time(6)),
    ('fmt:%H:%M', '06:00', time(6)),
    ('fmt:%M:%H', '06:50', ERROR),
    ('fmt:%H:%M', '3:00 am', ERROR),
    ('fmt:%H:%M', 'some night', ERROR),
    ('fmt:%H:%M', 'invalid', ERROR),
    ('fmt:%H:%M', True, ERROR),
    ('fmt:%H:%M', '', ERROR),
])
def test_cast_time(format, value, result):
    with warnings.catch_warnings():
        warnings.simplefilter("error" if not format.startswith('fmt:') else "ignore")
        assert types.cast_time(format, value) == result
