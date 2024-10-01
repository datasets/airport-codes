# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tableschema import constraints


# Tests

@pytest.mark.parametrize('constraint, value, result', [
    (0, [1], False),
    (1, [1], True),
    (2, [1], True),
])
def test_check_maxLength(constraint, value, result):
    assert constraints.check_maxLength(constraint, value) == result
