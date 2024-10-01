# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
import io
import sys
import six
import json
import requests
from copy import deepcopy
from importlib.util import find_spec
from . import exceptions
from . import config


# Retrieve descriptor

def retrieve_descriptor(source):

    try:
        # Inline
        if isinstance(source, (dict, list)):
            return deepcopy(source)

        # String
        if isinstance(source, six.string_types):
            # Remote
            if six.moves.urllib.parse.urlparse(source).scheme in config.REMOTE_SCHEMES:
                return requests.get(source).json()

            # Local
            with io.open(source, encoding='utf-8') as file:
                return json.load(file)

        # Stream
        return json.load(source)

    except Exception:
        raise exceptions.LoadError('Can\'t load descriptor')


# Expand descriptor

def expand_schema_descriptor(descriptor):
    if isinstance(descriptor, dict):
        descriptor = deepcopy(descriptor)
        for field in descriptor.get('fields', []):
            field.setdefault('type', config.DEFAULT_FIELD_TYPE)
            field.setdefault('format', config.DEFAULT_FIELD_FORMAT)
        descriptor.setdefault('missingValues', config.DEFAULT_MISSING_VALUES)
    return descriptor


def expand_field_descriptor(descriptor):
    descriptor = deepcopy(descriptor)
    descriptor.setdefault('type', config.DEFAULT_FIELD_TYPE)
    descriptor.setdefault('format', config.DEFAULT_FIELD_FORMAT)
    return descriptor


# Miscellaneous

def ensure_dir(path):
    """Ensure directory exists.

    Args:
        path(str): dir path

    """
    dirpath = os.path.dirname(path)
    if dirpath and not os.path.exists(dirpath):
        os.makedirs(dirpath)


def normalize_value(value):
    """Convert value to string and make it lower cased.
    """
    cast = str
    if six.PY2:
        cast = unicode  # noqa
    return cast(value).lower()


def default_exc_handler(exc, *args, **kwargs):
    """Default exception handler function: raise exc, ignore other arguments.
    """
    raise exc


class PluginImporter(object):
    """Plugin importer.

    Example:
        Add to myapp.plugins something like this:
        ```
        importer = PluginImporter(virtual='myapp.plugins.', actual='myapp_')
        importer.register()
        del PluginImporter
        del importer
        ```

    """

    # Public

    def __init__(self, virtual, actual):
        self.__virtual = virtual
        self.__actual = actual

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return (self.virtual == other.virtual and
                self.actual == other.actual)

    @property
    def virtual(self):
        return self.__virtual

    @property
    def actual(self):
        return self.__actual

    def register(self):
        if self not in sys.meta_path:
            sys.meta_path.append(self)

    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith(self.virtual):
            # Transform the module name
            transformed_name = fullname.replace(self.virtual, self.actual)
            return find_spec(transformed_name)
        return None

