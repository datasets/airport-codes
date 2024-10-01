# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
from collections import OrderedDict
from ..parser import Parser
from .. import exceptions


# Module API

class InlineParser(Parser):
    """Parser to provide support for python inline lists.
    """

    # Public

    options = []

    def __init__(self, loader, force_parse=False):
        self.__loader = loader
        self.__force_parse = force_parse
        self.__extended_rows = None
        self.__encoding = None
        self.__source = None

    @property
    def closed(self):
        return False

    def open(self, source, encoding=None):
        if hasattr(source, '__next__' if six.PY3 else 'next'):
            message = 'Only callable returning an iterator is supported'
            raise exceptions.SourceError(message)
        self.close()
        self.__source = source
        self.__encoding = encoding
        self.reset()

    def close(self):
        pass

    def reset(self):
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        items = self.__source
        if not hasattr(items, '__iter__'):
            items = items()
        for row_number, item in enumerate(items, start=1):
            if isinstance(item, (tuple, list)):
                yield (row_number, None, list(item))
            elif isinstance(item, dict):
                keys = []
                values = []
                iterator = item.keys()
                if not isinstance(item, OrderedDict):
                    iterator = sorted(iterator)
                for key in iterator:
                    keys.append(key)
                    values.append(item[key])
                yield (row_number, list(keys), list(values))
            else:
                if not self.__force_parse:
                    message = 'Inline data item has to be tuple, list or dict'
                    raise exceptions.SourceError(message)
                yield (row_number, None, [])
