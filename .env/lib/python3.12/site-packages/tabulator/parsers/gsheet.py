# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import re
from ..stream import Stream
from ..parser import Parser


# Module API

class GsheetParser(Parser):
    """Parser to parse Google Spreadsheets.
    """

    # Public

    options = []

    def __init__(self, loader, force_parse=False):
        self.__loader = loader
        self.__force_parse = force_parse
        self.__stream = None
        self.__encoding = None

    @property
    def closed(self):
        return self.__stream is None or self.__stream.closed

    def open(self, source, encoding=None):
        self.close()
        url = 'https://docs.google.com/spreadsheets/d/%s/export?format=csv&id=%s'
        match = re.search(r'.*/d/(?P<key>[^/]+)/.*?(?:gid=(?P<gid>\d+))?$', source)
        key, gid = '', ''
        if match:
            key = match.group('key')
            gid = match.group('gid')
        url = url % (key, key)
        if gid:
            url = '%s&gid=%s' % (url, gid)
        self.__stream = Stream(
            url, format='csv', encoding=encoding, force_parse=self.__force_parse).open()
        self.__extended_rows = self.__stream.iter(extended=True)
        self.__encoding = encoding

    def close(self):
        if not self.closed:
            self.__stream.close()

    def reset(self):
        self.__stream.reset()
        self.__extended_rows = self.__stream.iter(extended=True)

    @property
    def encoding(self):
        return self.__encoding

    @property
    def extended_rows(self):
        return self.__extended_rows
