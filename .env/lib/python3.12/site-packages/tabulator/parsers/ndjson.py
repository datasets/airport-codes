# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import jsonlines
from ..parser import Parser
from .. import exceptions
from .. import helpers


# Module API

class NDJSONParser(Parser):
    """Parser to parse NDJSON data format.

    See: http://specs.okfnlabs.org/ndjson/
    """

    # Public

    options = []

    def __init__(self, loader, force_parse=False):
        self.__loader = loader
        self.__force_parse = force_parse
        self.__extended_rows = None
        self.__encoding = None
        self.__chars = None

    @property
    def closed(self):
        return self.__chars is None or self.__chars.closed

    def open(self, source, encoding=None):
        self.close()
        self.__chars = self.__loader.load(source, encoding=encoding)
        self.__encoding = getattr(self.__chars, 'encoding', encoding)
        if self.__encoding:
            self.__encoding.lower()
        self.reset()

    def close(self):
        if not self.closed:
            self.__chars.close()

    def reset(self):
        helpers.reset_stream(self.__chars)
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        rows = jsonlines.Reader(self.__chars)
        for row_number, row in enumerate(rows, start=1):
            if isinstance(row, (tuple, list)):
                yield row_number, None, list(row)
            elif isinstance(row, dict):
                keys, values = zip(*sorted(row.items()))
                yield (row_number, list(keys), list(values))
            else:
                if not self.__force_parse:
                    raise exceptions.SourceError('JSON item has to be list or dict')
                yield (row_number, None, [])
