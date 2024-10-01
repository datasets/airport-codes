# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
import warnings
from . import config
from .table import Table


# Module API

def infer(source, headers=1, limit=100, confidence=0.75,
          missing_values=config.DEFAULT_MISSING_VALUES,
          guesser_cls=None, resolver_cls=None,
          **options):
    """Infer source schema.

    # Arguments
        source (any): source as path, url or inline data
        headers (int/str[]): headers rows number or headers list
        confidence (float): how many casting errors are allowed (as a ratio, between 0 and 1)
        missing_values (str[]): list of missing values (by default `['']`)
        guesser_cls (class): you can implement inferring strategies by
            providing type-guessing and type-resolving classes [experimental]
        resolver_cls (class): you can implement inferring strategies by
            providing type-guessing and type-resolving classes [experimental]

    # Raises
        TableSchemaException: raises any error that occurs during the process

    # Returns
        dict: returns schema descriptor

    """

    # Deprecated arguments order
    is_string = lambda value: isinstance(value, six.string_types)
    if isinstance(source, list) and all(map(is_string, source)):
        warnings.warn('Correct arguments order infer(source, headers)', UserWarning)
        source, headers = headers, source

    table = Table(source, headers=headers, sample_size=limit, **options)
    descriptor = table.infer(limit=limit, confidence=confidence,
        missing_values=missing_values, guesser_cls=guesser_cls,
        resolver_cls=resolver_cls)
    return descriptor
