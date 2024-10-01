# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from decimal import Decimal

import pytest
from tableschema import types
from tableschema.config import ERROR


# Tests

@pytest.mark.parametrize('format, value, result, options', [
    ('default', 1, 1, {}),
    ('default', 1 << 63, 1 << 63, {}),
    ('default', '1', 1, {}),
    ('default', 1.0, 1, {}),
    ('default', Decimal('1.0'), 1, {}),
    ('default', '1$', 1, {'bareNumber': False}),
    ('default', 'ab1$', 1, {'bareNumber': False}),
    ('default', True, ERROR, {}),
    ('default', False, ERROR, {}),
    ('default', 3.14, ERROR, {}),
    ('default', '3.14', ERROR, {}),
    ('default', Decimal('3.14'), ERROR, {}),
    ('default', '', ERROR, {}),
])
def test_cast_integer(format, value, result, options):
    assert types.cast_integer(format, value, **options) == result
