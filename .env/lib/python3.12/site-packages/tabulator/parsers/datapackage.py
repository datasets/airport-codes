# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import six
import datapackage
from ..parser import Parser
from .. import exceptions


# Module API

class DataPackageParser(Parser):
    """Parser to extract data from Tabular Data Packages.
    """

    # Public

    options = [
        'resource',
    ]

    def __init__(self, loader, force_parse=False, resource=0):
        self.__force_parse = force_parse
        self.__resource_pointer = resource
        self.__extended_rows = None
        self.__encoding = None
        self.__fragment = None
        self.__resource = None

    @property
    def closed(self):
        return self.__extended_rows is None

    def open(self, source, encoding=None):
        self.close()
        package = datapackage.DataPackage(source)
        if isinstance(self.__resource_pointer, six.string_types):
            self.__resource = package.get_resource(self.__resource_pointer)
        else:
            try:
                self.__resource = package.resources[self.__resource_pointer]
            except (TypeError, IndexError):
                pass
        if not self.__resource:
            message = 'Data package "%s" doesn\'t have resource "%s"'
            raise exceptions.SourceError(message % (source, self.__resource_pointer))
        self.__resource.infer()
        self.__encoding = self.__resource.descriptor.get('encoding')
        self.__fragment = self.__resource.name
        self.reset()

    def close(self):
        if not self.closed:
            self.__extended_rows = None

    def reset(self):
        self.__extended_rows = self.__iter_extended_rows()

    @property
    def encoding(self):
        return self.__encoding

    @property
    def fragment(self):
        return self.__fragment

    @property
    def extended_rows(self):
        return self.__extended_rows

    # Private

    def __iter_extended_rows(self):
        for row_number, headers, row in self.__resource.iter(extended=True):
            yield (row_number - 1, headers, row)
