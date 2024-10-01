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
    ('default', [], []),
    ('default', (), []),
    ('default', '[]', []),
    ('default', ['key', 'value'], ['key', 'value']),
    ('default', ('key', 'value'), ['key', 'value']),
    ('default', '["key", "value"]', ['key', 'value']),
    ('default', {'key': 'value'}, ERROR),
    ('default', '{"key": "value"}', ERROR),
    ('default', 'string', ERROR),
    ('default', 1, ERROR),
    ('default', '3.14', ERROR),
    ('default', '', ERROR),
])
def test_cast_array(format, value, result):
    assert types.cast_array(format, value) == result
