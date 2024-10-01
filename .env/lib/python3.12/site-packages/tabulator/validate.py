# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from . import config
from . import helpers
from . import exceptions


# Module API

def validate(source, scheme=None, format=None):
    """Check if tabulator is able to load the source.

    # Arguments
        source (Union[str, IO]): The source path or IO object.
        scheme (str, optional): The source scheme. Auto-detect by default.
        format (str, optional): The source file format. Auto-detect by default.

    # Raises
        SchemeError: The file scheme is not supported.
        FormatError: The file format is not supported.

    # Returns
        bool: Whether tabulator is able to load the source file.

    """

    # Get scheme and format
    detected_scheme, detected_format = helpers.detect_scheme_and_format(source)
    scheme = scheme or detected_scheme
    format = format or detected_format

    # Validate scheme and format
    if scheme is not None:
        if scheme not in config.LOADERS:
            raise exceptions.SchemeError('Scheme "%s" is not supported' % scheme)
    if format not in config.PARSERS:
        raise exceptions.FormatError('Format "%s" is not supported' % format)

    return True
