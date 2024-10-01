# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from .package import Package


# Module API

def infer(pattern, base_path=None):
    """Infer a data package descriptor.

    > Argument `pattern` works only for local files

    # Arguments
        pattern (str): glob file pattern

    # Returns
        dict: returns data package descriptor

    """
    package = Package({}, base_path=base_path)
    descriptor = package.infer(pattern)
    return descriptor
