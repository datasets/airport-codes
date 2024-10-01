# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tableschema import constraints


# Tests

@pytest.mark.parametrize('constraint, value, result', [
    (0, [1], True),
    (1, [1], True),
    (2, [1], False),
])
def test_check_minLength(constraint, value, result):
    assert constraints.check_minLength(constraint, value) == result
