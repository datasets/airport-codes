# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import warnings
import pytest
from datetime import datetime
from tableschema import types
from tableschema.config import ERROR


# Tests

@pytest.mark.parametrize('format, value, result', [
    ('default', datetime(2014, 1, 1, 6), datetime(2014, 1, 1, 6)),
    ('default', '2014-01-01T06:00:00Z', datetime(2014, 1, 1, 6)),
    ('default', 'Mon 1st Jan 2014 9 am', ERROR),
    ('default', 'invalid', ERROR),
    ('default', True, ERROR),
    ('default', '', ERROR),
    ('any', datetime(2014, 1, 1, 6), datetime(2014, 1, 1, 6)),
    ('any', '10th Jan 1969 9 am', datetime(1969, 1, 10, 9)),
    ('any', 'invalid', ERROR),
    ('any', True, ERROR),
    ('any', '', ERROR),
    ('%d/%m/%y %H:%M', datetime(2006, 11, 21, 16, 30), datetime(2006, 11, 21, 16, 30)),
    ('%d/%m/%y %H:%M', '21/11/06 16:30', datetime(2006, 11, 21, 16, 30)),
    ('%H:%M %d/%m/%y', '21/11/06 16:30', ERROR),
    ('%d/%m/%y %H:%M', 'invalid', ERROR),
    ('%d/%m/%y %H:%M', True, ERROR),
    ('%d/%m/%y %H:%M', '', ERROR),
    ('invalid', '21/11/06 16:30', ERROR),
    # Deprecated
    ('fmt:%d/%m/%y %H:%M', datetime(2006, 11, 21, 16, 30), datetime(2006, 11, 21, 16, 30)),
    ('fmt:%d/%m/%y %H:%M', '21/11/06 16:30', datetime(2006, 11, 21, 16, 30)),
    ('fmt:%H:%M %d/%m/%y', '21/11/06 16:30', ERROR),
    ('fmt:%d/%m/%y %H:%M', 'invalid', ERROR),
    ('fmt:%d/%m/%y %H:%M', True, ERROR),
    ('fmt:%d/%m/%y %H:%M', '', ERROR),
])
def test_cast_datetime(format, value, result):
    with warnings.catch_warnings():
        warnings.simplefilter("error" if not format.startswith('fmt:') else "ignore")
        assert types.cast_datetime(format, value) == result
