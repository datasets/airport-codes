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
    ('default', [2000, 10], (2000, 10)),
    ('default', (2000, 10), (2000, 10)),
    ('default', '2000-10', (2000, 10)),
    ('default', (2000, 10, 20), ERROR),
    ('default', '2000-13-20', ERROR),
    ('default', '2000-13', ERROR),
    ('default', '2000-0', ERROR),
    ('default', '13', ERROR),
    ('default', -10, ERROR),
    ('default', 20, ERROR),
    ('default', '3.14', ERROR),
    ('default', '', ERROR),
])
def test_cast_yearmonth(format, value, result):
    assert types.cast_yearmonth(format, value) == result
