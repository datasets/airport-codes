# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
from ..loader import Loader
from .. import exceptions
from .. import helpers
from .. import config


# Module API

class LocalLoader(Loader):
    """Loader to load source from filesystem.
    """

    # Public

    options = []

    def __init__(self, bytes_sample_size=config.DEFAULT_BYTES_SAMPLE_SIZE):
        self.__bytes_sample_size = bytes_sample_size
        self.__stats = None

    def attach_stats(self, stats):
        self.__stats = stats

    def load(self, source, mode='t', encoding=None):

        # Prepare source
        scheme = 'file://'
        if source.startswith(scheme):
            source = source.replace(scheme, '', 1)

        # Prepare bytes
        try:
            bytes = io.open(source, 'rb')
            if self.__stats:
                bytes = helpers.BytesStatsWrapper(bytes, self.__stats)
        except IOError as exception:
            raise exceptions.LoadingError(str(exception))

        # Return bytes
        if mode == 'b':
            return bytes

        # Detect encoding
        if self.__bytes_sample_size:
            sample = bytes.read(self.__bytes_sample_size)
            bytes.seek(0)
            encoding = helpers.detect_encoding(sample, encoding)

        # Prepare chars
        chars = io.TextIOWrapper(bytes, encoding)

        return chars
