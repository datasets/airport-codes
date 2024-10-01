# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tableschema import types
from tableschema.config import ERROR


# Tests

@pytest.mark.parametrize('format, value, result, options', [
    ('default', True, True, {}),
    ('default', 'true', True, {}),
    ('default', 'True', True, {}),
    ('default', 'TRUE', True, {}),
    ('default', '1', True, {}),
    ('default', 'yes', True, {'trueValues': ['yes']}),
    ('default', False, False, {}),
    ('default', 'false', False, {}),
    ('default', 'False', False, {}),
    ('default', 'FALSE', False, {}),
    ('default', '0', False, {}),
    ('default', 'no', False, {'falseValues': ['no']}),
    ('default', 't', ERROR, {}),
    ('default', 'YES', ERROR, {}),
    ('default', 'Yes', ERROR, {}),
    ('default', 'f', ERROR, {}),
    ('default', 'NO', ERROR, {}),
    ('default', 'No', ERROR, {}),
    ('default', 0, ERROR, {}),
    ('default', 1, ERROR, {}),
    ('default', 0, False, {'falseValues': [0], 'trueValues': [1]}),
    ('default', 1, True, {'falseValues': [0], 'trueValues': [1]}),
    ('default', '3.14', ERROR, {}),
    ('default', '', ERROR, {}),
    ('default', 'Yes', ERROR, {'trueValues': ['yes']}),
    ('default', 'No', ERROR, {'falseValues': ['no']}),
])
def test_cast_boolean(format, value, result, options):
    assert types.cast_boolean(format, value, **options) == result
