# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals


# Module API

class TabulatorException(Exception):
    """Base class for all tabulator exceptions.
    """
    pass


class SourceError(TabulatorException):
    """The source file could not be parsed correctly.
    """
    pass


class SchemeError(TabulatorException):
    """The file scheme is not supported.
    """
    pass


class FormatError(TabulatorException):
    """The file format is unsupported or invalid.
    """
    pass


class EncodingError(TabulatorException):
    """Encoding error
    """
    pass


class CompressionError(TabulatorException):
    """Compression error
    """
    pass


# Deprecated

OptionsError = TabulatorException
ResetError = TabulatorException


class IOError(SchemeError):
    """Local loading error
    """
    pass


class LoadingError(IOError):
    """Local loading error
    """
    pass


class HTTPError(LoadingError):
    """Remote loading error
    """
    pass
