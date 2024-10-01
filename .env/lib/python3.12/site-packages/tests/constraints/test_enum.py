# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tableschema import constraints


# Tests

@pytest.mark.parametrize('constraint, value, result', [
    ([1, 2], 1, True),
    ([0, 2], 1, False),
    ([], 1, False),
])
def test_check_enum(constraint, value, result):
    assert constraints.check_enum(constraint, value) == result
