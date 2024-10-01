# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tableschema import constraints
import re

# Tests

@pytest.mark.parametrize('constraint, value, result', [
    ('^test$', 'test', True),
    ('^test$', 'TEST', False),
    (re.compile('^test$'), 'test', True),
    (re.compile('^test$'), 'TEST', False),
])
def test_check_pattern(constraint, value, result):
    assert constraints.check_pattern(constraint, value) == result
