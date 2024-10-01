# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from .package import Package


# Module API

def validate(descriptor):
    """Validate a data package descriptor.

    # Arguments
        descriptor (str/dict): package descriptor (one of):
          - local path
          - remote url
          - object

    # Raises
        ValidationError: raises on invalid

    # Returns
        bool: returns true on valid

    """
    Package(descriptor, strict=True)
    return True
