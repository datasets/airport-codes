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
    ('default', 'string', 'string'),
    ('default', '', ''),
    ('default', 0, ERROR),
    ('uri', 'http://google.com', 'http://google.com'),
    ('uri', '://no-scheme.test', ERROR),
    ('uri', 'string', ERROR),
    ('uri', '', ERROR),
    ('uri', 0, ERROR),
    ('email', 'name@gmail.com', 'name@gmail.com'),
    ('email', 'http://google.com', ERROR),
    ('email', 'string', ERROR),
    ('email', '', ERROR),
    ('email', 0, ERROR),
    ('binary', 'dGVzdA==', 'dGVzdA=='),
    ('binary', '', ''),
    ('binary', 'string', ERROR),
    ('binary', 0, ERROR),
])
def test_cast_string(format, value, result):
    assert types.cast_string(format, value) == result
