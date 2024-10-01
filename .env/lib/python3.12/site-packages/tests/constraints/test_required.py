# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from tableschema import constraints


# Tests

@pytest.mark.parametrize('constraint, value, result', [
    (False, 1, True),
    (True, 0, True),
    (True, None, False),
])
def test_check_required(constraint, value, result):
    assert constraints.check_required(constraint, value) == result
