# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
import six
import uuid
import base64
import rfc3986.exceptions
import rfc3986.validators
import rfc3986.uri
from ..config import ERROR


# Module API

def cast_string(format, value, **options):
    if not isinstance(value, six.string_types):
        return ERROR
    if format in _SIMPLE_FORMATS:
        return value
    if format == 'uri':
        uri = _uri_from_string(value)
        try:
            _uri_validator.validate(uri)
        except rfc3986.exceptions.ValidationError:
            return ERROR
    elif format == 'email':
        if not re.match(_EMAIL_PATTERN, value):
            return ERROR
    elif format == 'uuid':
        try:
            uuid.UUID(value, version=4)
        except Exception:
            return ERROR
    elif format == 'binary':
        try:
            base64.b64decode(value)
        except Exception:
            return ERROR
    return value


# Internal

_SIMPLE_FORMATS = {'default', None}
_EMAIL_PATTERN = re.compile(r'[^@]+@[^@]+\.[^@]+')
_uri_from_string = rfc3986.uri.URIReference.from_string
_uri_validator = rfc3986.validators.Validator().require_presence_of('scheme')
