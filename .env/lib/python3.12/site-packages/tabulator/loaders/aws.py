# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import boto3
from six.moves.urllib.parse import urlparse
from ..loader import Loader
from .. import exceptions
from .. import helpers
from .. import config


# Module API

class AWSLoader(Loader):
    """Loader to load source from the AWS.
    """

    # Public

    remote = True
    options = [
        's3_endpoint_url',
    ]

    def __init__(self,
                 bytes_sample_size=config.DEFAULT_BYTES_SAMPLE_SIZE,
                 s3_endpoint_url=None):
        self.__bytes_sample_size = bytes_sample_size
        self.__s3_endpoint_url = (
            s3_endpoint_url or
            os.environ.get('S3_ENDPOINT_URL') or
            config.S3_DEFAULT_ENDPOINT_URL)
        self.__s3_client = boto3.client('s3', endpoint_url=self.__s3_endpoint_url)
        self.__stats = None

    def attach_stats(self, stats):
        self.__stats = stats

    def load(self, source, mode='t', encoding=None):

        # Prepare bytes
        try:
            parts = urlparse(source, allow_fragments=False)
            response = self.__s3_client.get_object(Bucket=parts.netloc, Key=parts.path[1:])
            # https://github.com/frictionlessdata/tabulator-py/issues/271
            bytes = io.BufferedRandom(io.BytesIO())
            bytes.write(response['Body'].read())
            bytes.seek(0)
            if self.__stats:
                bytes = helpers.BytesStatsWrapper(bytes, self.__stats)
        except Exception as exception:
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
