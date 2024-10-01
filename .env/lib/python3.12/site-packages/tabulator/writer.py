# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from six import add_metaclass
from abc import ABCMeta, abstractmethod


# Module API

@add_metaclass(ABCMeta)
class Writer(object):
    """Abstract class implemented by the data writers.

    The writers inherit and implement this class' methods to add support for a
    new file destination.

    # Arguments
        **options (dict): Writer options.

    """

    # Public

    options = []

    def __init__(self, **options):
        pass

    @abstractmethod
    def write(self, source, target, headers, encoding=None):
        """Writes source data to target.

        # Arguments
            source (str): Source data.
            target (str): Write target.
            headers (List[str]): List of header names.
            encoding (str, optional): Source file encoding.

        # Returns
            count (int?): Written rows count if available

        """
        pass
