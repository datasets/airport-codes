# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import warnings
import pytest
from datetime import date, datetime
from tableschema import types
from tableschema.config import ERROR


# Tests

@pytest.mark.parametrize('format, value, result', [
    ('default', date(2019, 1, 1), date(2019, 1, 1)),
    ('default', '2019-01-01', date(2019, 1, 1)),
    ('default', '10th Jan 1969', ERROR),
    ('default', 'invalid', ERROR),
    ('default', True, ERROR),
    ('default', '', ERROR),
    ('default', datetime(2018, 1, 1), date(2018, 1, 1)),
    ('default', datetime(2018, 3, 1, 8, 30, 23), ERROR),
    ('any', date(2019, 1, 1), date(2019, 1, 1)),
    ('any', '2019-01-01', date(2019, 1, 1)),
    ('any', '10th Jan 1969', date(1969, 1, 10)),
    ('any', '10th Jan nineteen sixty nine', ERROR),
    ('any', 'invalid', ERROR),
    ('any', True, ERROR),
    ('any', '', ERROR),
    ('%d/%m/%y', date(2019, 1, 1), date(2019, 1, 1)),
    ('%d/%m/%y', '21/11/06', date(2006, 11, 21)),
    ('%y/%m/%d', '21/11/06 16:30', ERROR),
    ('%d/%m/%y', 'invalid', ERROR),
    ('%d/%m/%y', True, ERROR),
    ('%d/%m/%y', '', ERROR),
    ('invalid', '21/11/06 16:30', ERROR),
    # Deprecated
    ('fmt:%d/%m/%y', date(2019, 1, 1), date(2019, 1, 1)),
    ('fmt:%d/%m/%y', '21/11/06', date(2006, 11, 21)),
    ('fmt:%y/%m/%d', '21/11/06 16:30', ERROR),
    ('fmt:%d/%m/%y', 'invalid', ERROR),
    ('fmt:%d/%m/%y', True, ERROR),
    ('fmt:%d/%m/%y', '', ERROR),
])
def test_cast_date(format, value, result):
    with warnings.catch_warnings():
        warnings.simplefilter("error" if not format.startswith('fmt:') else "ignore")
        assert types.cast_date(format, value) == result
