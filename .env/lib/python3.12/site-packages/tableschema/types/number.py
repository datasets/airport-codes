# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import re
import six
from decimal import Decimal
from ..config import ERROR


# Module API

def cast_number(format, value, **options):
    if isinstance(value, six.string_types):
        group_char = options.get('groupChar', _DEFAULT_GROUP_CHAR)
        decimal_char = options.get('decimalChar', _DEFAULT_DECIMAL_CHAR)
        value = _RE_WHITESPACE.sub('', value)
        if decimal_char != '.':
            if group_char:
                value = value.replace(decimal_char, '__decimal_char__')
                value = value.replace(group_char, '')
                value = value.replace('__decimal_char__', '.')
            else:
                value = value.replace(decimal_char, '__decimal_char__')
                value = value.replace('__decimal_char__', '.')
        elif group_char:
            value = value.replace(group_char, '')
                
        if not options.get('bareNumber', _DEFAULT_BARE_NUMBER):
            value = _RE_BARE_NUMBER.sub('', value)
    elif isinstance(value, Decimal):
        return value
    elif not isinstance(value, six.integer_types + (float,)):
        return ERROR
    elif value is True or value is False:
        return ERROR
    else:
        value = str(value)
    try:
        value = Decimal(value)
    except Exception:
        return ERROR
    return value


# Internal

_RE_WHITESPACE = re.compile(r'\s')
_RE_BARE_NUMBER = re.compile(r'((^\D*)|(\D*$))')
_DEFAULT_GROUP_CHAR = ''
_DEFAULT_DECIMAL_CHAR = '.'
_DEFAULT_BARE_NUMBER = True
