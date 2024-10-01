# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import six
import unicodecsv
from ..writer import Writer
from .. import helpers


# Module API

class CSVWriter(Writer):
    """CSV writer.
    """

    # Public

    options = [
        'delimiter',
    ]

    def __init__(self, **options):

        # Make bytes
        if six.PY2:
            for key, value in options.items():
                if isinstance(value, six.string_types):
                    options[key] = str(value)

        # Set attributes
        self.__options = options

    def write(self, source, target, headers, encoding=None):
        helpers.ensure_dir(target)
        count = 0
        with io.open(target, 'wb') as file:
            writer = unicodecsv.writer(file, encoding=encoding, **self.__options)
            if headers:
                writer.writerow(headers)
            for row in source:
                count += 1
                writer.writerow(row)
        return count
