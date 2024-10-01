# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import ijson
from ..parser import Parser
from .. import exceptions
from .. import helpers


# Module API

class JSONParser(Parser):
    """Parser to parse JSON data format.
    """

    # Public

    options = [
        'property',
    ]

    def __init__(self, loader, force_parse=False, property=None):
        self.__loader = loader
        self.__property = property
        self.__force_parse = force_parse
        self.__extended_rows = None
        self.__encoding = None
        self.__bytes = None

    @property
    def closed(self):
        return self.__bytes is None or self.__bytes.closed

    def open(self, source, encoding=None):
        self.close()
        self.__encoding = encoding
        self.__bytes = self.__loader.load(source, mode='b', encoding=encoding)
        if self.__encoding:
            self.__encoding.lower()
        self.reset()

    def close(self):
        if not self.closed:
            self.__bytes.close()

    def reset(self):
        helpers.reset_stream(self.__bytes)
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        path = 'item'
        if self.__property is not None:
            path = '%s.item' % self.__property
        items = ijson.items(self.__bytes, path)
        for row_number, item in enumerate(items, start=1):
            if isinstance(item, (tuple, list)):
                yield (row_number, None, list(item))
            elif isinstance(item, dict):
                keys = []
                values = []
                for key in sorted(item.keys()):
                    keys.append(key)
                    values.append(item[key])
                yield (row_number, list(keys), list(values))
            else:
                if not self.__force_parse:
                    message = 'JSON item has to be list or dict'
                    raise exceptions.SourceError(message)
                yield (row_number, None, [])
