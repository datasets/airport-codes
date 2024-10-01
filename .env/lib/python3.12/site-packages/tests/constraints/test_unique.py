# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tableschema import constraints


# Tests

@pytest.mark.parametrize('constraint, value, result', [
    (False, 'any', True),
    (True, 'any', True),
])
def test_check_unique(constraint, value, result):
    assert constraints.check_unique(constraint, value) == result
