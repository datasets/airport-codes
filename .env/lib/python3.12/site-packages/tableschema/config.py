# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import io
import os


# Module API

VERSION = io.open(os.path.join(os.path.dirname(__file__), 'VERSION')).read().strip()
ERROR = 'tableschema.error'
DEFAULT_FIELD_TYPE = 'string'
DEFAULT_FIELD_FORMAT = 'default'
DEFAULT_MISSING_VALUES = ['']
REMOTE_SCHEMES = ['http', 'https', 'ftp', 'ftps', 's3']
