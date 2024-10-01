# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from .schema import Schema


# Module API


def validate(descriptor):
    """Validate descriptor

    # Arguments
        dict: descriptor

    # Raises
        ValidationError: on validation errors

    # Returns
        bool: True

    """
    Schema(descriptor, strict=True)
    return True
