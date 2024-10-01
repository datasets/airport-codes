# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
COMPILED_RE = type(re.compile(""))


# Module API

def check_pattern(constraint, value):
    if value is None:
        return True
    if not isinstance(constraint, COMPILED_RE):
        regex = re.compile('^{0}$'.format(constraint))
    else:
        regex = constraint
    match = regex.match(value)
    if match:
        return True
    return False
