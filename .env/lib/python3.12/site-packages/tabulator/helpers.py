# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
import six
import codecs
import hashlib
from copy import copy
from importlib import import_module
from six.moves.urllib.parse import parse_qs, urlparse, urlunparse
from . import exceptions
from . import config


# Module API

def detect_scheme_and_format(source):
    """Detect scheme and format based on source and return as a tuple.

    Scheme is a minimum 2 letters before `://` (will be lower cased).
    For example `http` from `http://example.com/table.csv`

    """

    # Scheme: stream
    if hasattr(source, 'read'):
        return ('stream', None)

    # Format: inline
    if not isinstance(source, six.string_types):
        return (None, 'inline')

    # Format: gsheet
    if 'docs.google.com/spreadsheets' in source:
        if 'export' not in source and 'pub' not in source:
            return (None, 'gsheet')
        elif 'csv' in source:
            return ('https', 'csv')

    # Format: sql
    for sql_scheme in config.SQL_SCHEMES:
        if source.startswith('%s://' % sql_scheme):
            return (None, 'sql')

    # General
    if source.startswith('text://'):
        return ('text', None)
    parsed = urlparse(source)
    scheme = parsed.scheme.lower()
    if len(scheme) < 2:
        scheme = config.DEFAULT_SCHEME
    format = os.path.splitext(parsed.path or parsed.netloc)[1][1:].lower() or None
    if format is None:
        # Test if query string contains a "format=" parameter.
        query_string = parse_qs(parsed.query)
        query_string_format = query_string.get("format")
        if query_string_format is not None and len(query_string_format) == 1:
            format = query_string_format[0]

    # Format: datapackage
    if parsed.path.endswith('datapackage.json'):
        return (None, 'datapackage')

    return (scheme, format)


# TODO: consider merging cp1252/iso8859-1
def detect_encoding(sample, encoding=None):
    """Detect encoding of a byte string sample.
    """
    # To reduce tabulator import time
    try:
        from cchardet import detect
    except ImportError:
        from chardet import detect
    if encoding is not None:
        return normalize_encoding(sample, encoding)
    result = detect(sample)
    confidence = result['confidence'] or 0
    encoding = result['encoding'] or 'ascii'
    encoding = normalize_encoding(sample, encoding)
    if confidence < config.ENCODING_CONFIDENCE:
        encoding = config.DEFAULT_ENCODING
    if encoding == 'ascii':
        encoding = config.DEFAULT_ENCODING
    return encoding


def normalize_encoding(sample, encoding):
    """Normalize encoding including 'utf-8-sig', 'utf-16-be', utf-16-le tweaks.
    """
    encoding = codecs.lookup(encoding).name
    # Work around 'Incorrect detection of utf-8-sig encoding'
    # <https://github.com/PyYoshi/cChardet/issues/28>
    if encoding == 'utf-8':
        if sample.startswith(codecs.BOM_UTF8):
            encoding = 'utf-8-sig'
    # Use the BOM stripping name (without byte-order) for UTF-16 encodings
    elif encoding == 'utf-16-be':
        if sample.startswith(codecs.BOM_UTF16_BE):
            encoding = 'utf-16'
    elif encoding == 'utf-16-le':
        if sample.startswith(codecs.BOM_UTF16_LE):
            encoding = 'utf-16'
    return encoding


def detect_html(text):
    """Detect if text is HTML.
    """
    pattern = re.compile('\\s*<(!doctype|html)', re.IGNORECASE)
    return bool(pattern.match(text))


def reset_stream(stream):
    """Reset stream pointer to the first element.

    If stream is not seekable raise Exception.

    """
    try:
        position = stream.tell()
    except Exception:
        position = True
    if position != 0:
        try:
            stream.seek(0)
        except Exception:
            message = 'It\'s not possible to reset this stream'
            raise exceptions.TabulatorException(message)


def ensure_dir(path):
    """Ensure path directory exists.
    """
    dirpath = os.path.dirname(path)
    if dirpath and not os.path.exists(dirpath):
        os.makedirs(dirpath)


def requote_uri(uri):
    """Requote uri if it contains non-ascii chars, spaces etc.
    """
    # To reduce tabulator import time
    import requests.utils
    if six.PY2:
        def url_encode_non_ascii(bytes):
            pattern = '[\x80-\xFF]'
            replace = lambda c: ('%%%02x' % ord(c.group(0))).upper()
            return re.sub(pattern, replace, bytes)
        parts = urlparse(uri)
        uri = urlunparse(
            part.encode('idna') if index == 1
            else url_encode_non_ascii(part.encode('utf-8'))
            for index, part in enumerate(parts))
    return requests.utils.requote_uri(uri)


def import_attribute(path):
    """Import attribute by path like `package.module.attribute`
    """
    module_name, attribute_name = path.rsplit('.', 1)
    module = import_module(module_name)
    attribute = getattr(module, attribute_name)
    return attribute


def extract_options(options, names):
    """Return options for names and remove it from given options in-place.
    """
    result = {}
    for name, value in copy(options).items():
        if name in names:
            result[name] = value
            del options[name]
    return result


def stringify_value(value):
    """Convert any value to string.
    """
    if value is None:
        return u''
    isoformat = getattr(value, 'isoformat', None)
    if isoformat is not None:
        value = isoformat()
    return type(u'')(value)


class BytesStatsWrapper(object):
    """This class is intended to be used as

    stats = {'size': 0, 'hash': ''}
    bytes = BytesStatsWrapper(bytes, stats)

    It will be updating the stats during reading.

    """

    def __init__(self, bytes, stats):
        self.__hasher = getattr(hashlib, stats['hashing_algorithm'])()
        self.__bytes = bytes
        self.__stats = stats

    def __getattr__(self, name):
        return getattr(self.__bytes, name)

    @property
    def closed(self):
        return self.__bytes.closed

    def read1(self, size=None):
        chunk = self.__bytes.read1(size)
        self.__hasher.update(chunk)
        self.__stats['size'] += len(chunk)
        self.__stats['hash'] = self.__hasher.hexdigest()
        return chunk
