# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import six
import requests
from ..loader import Loader
from .. import exceptions
from .. import helpers
from .. import config


# Module API

class RemoteLoader(Loader):
    """Loader to load source from the web.
    """

    # Public

    remote = True
    options = [
        'http_session',
        'http_stream',
        'http_timeout',
    ]

    def __init__(self,
                 bytes_sample_size=config.DEFAULT_BYTES_SAMPLE_SIZE,
                 http_session=None,
                 http_stream=True,
                 http_timeout=None):

        # Create default session
        if not http_session:
            http_session = requests.Session()
            http_session.headers.update(config.HTTP_HEADERS)

        # No stream support
        if six.PY2:
            http_stream = False

        # Set attributes
        self.__bytes_sample_size = bytes_sample_size
        self.__http_session = http_session
        self.__http_stream = http_stream
        self.__http_timeout = http_timeout
        self.__stats = None

    def attach_stats(self, stats):
        self.__stats = stats

    def load(self, source, mode='t', encoding=None):

        # Prepare source
        source = helpers.requote_uri(source)

        # Prepare bytes
        try:
            bytes = _RemoteStream(source, self.__http_session, self.__http_timeout).open()
            if not self.__http_stream:
                buffer = io.BufferedRandom(io.BytesIO())
                buffer.write(bytes.read())
                buffer.seek(0)
                bytes = buffer
            if self.__stats:
                bytes = helpers.BytesStatsWrapper(bytes, self.__stats)
        except IOError as exception:
            raise exceptions.HTTPError(str(exception))

        # Return bytes
        if mode == 'b':
            return bytes

        # Detect encoding
        if self.__bytes_sample_size:
            sample = bytes.read(self.__bytes_sample_size)[:self.__bytes_sample_size]
            bytes.seek(0)
            encoding = helpers.detect_encoding(sample, encoding)

        # Prepare chars
        chars = io.TextIOWrapper(bytes, encoding)

        return chars


# Internal

class _RemoteStream(object):

    # It's possible to implement cache for bytes sample
    # size to prevent additional HTTP calls used in seek

    # Public

    remote = True

    def __init__(self, source, session, timeout):
        self.__source = source
        self.__session = session
        self.__timeout = timeout

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return True

    @property
    def closed(self):
        return self.__closed

    def open(self):
        self.__closed = False
        self.seek(0)
        return self

    def close(self):
        self.__closed = True

    def tell(self):
        return self.__response.raw.tell()

    def flush(self):
        pass

    def read(self, size=None):
        return self.__response.raw.read(size)

    def read1(self, size=None):
        return self.__response.raw.read(size)

    def seek(self, offset, whence=0):
        assert offset == 0
        assert whence == 0
        self.__response = self.__session.get(self.__source, stream=True, timeout=self.__timeout)
        self.__response.raise_for_status()
        self.__response.raw.decode_content = True
