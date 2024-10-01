# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import decimal


# Module API

def check_minimum(constraint, value):
    if value is None:
        return True
    try:
        if value >= constraint:
            return True
    except decimal.InvalidOperation:
        # For non-finite numbers NaN, INF and -INF
        # the constraint always is not satisfied
        return False
    return False
