# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import io
import os


# Module API

VERSION = io.open(os.path.join(os.path.dirname(__file__), 'VERSION')).read().strip()
REMOTE_SCHEMES = ['http', 'https', 'ftp', 'ftps', 's3']
TABULAR_FORMATS = ['csv', 'tsv', 'xls', 'xlsx']
DEFAULT_DATA_PACKAGE_PROFILE = 'data-package'
DEFAULT_RESOURCE_PROFILE = 'data-resource'
DEFAULT_FIELD_TYPE = 'string'
DEFAULT_FIELD_FORMAT = 'default'
DEFAULT_MISSING_VALUES = ['']
DEFAULT_DIALECT = {
    'delimiter': ',',
    'doubleQuote': True,
    'lineTerminator': '\r\n',
    'quoteChar': '"',
    'skipInitialSpace': True,
    'header': True,
    'caseSensitiveHeader': False,
}
HTTP_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) ' +
                'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                'Chrome/54.0.2840.87 Safari/537.36'
}
