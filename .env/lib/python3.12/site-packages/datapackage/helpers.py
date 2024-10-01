# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import io
import os
import re
import six
import json
import requests
import jsonpointer
from . import config
from . import exceptions


# Get descriptor base path

def get_descriptor_base_path(descriptor):
    """Get descriptor base path if string or return None.
    """

    # Infer from path/url
    if isinstance(descriptor, six.string_types):
        if os.path.exists(descriptor):
            base_path = os.path.dirname(os.path.abspath(descriptor))
        else:
            # suppose descriptor is a URL
            base_path = os.path.dirname(descriptor)

    # Current dir by default
    else:
        base_path = '.'

    return base_path


# Retrieve descriptor

def retrieve_descriptor(descriptor):
    """Retrieve descriptor.
    """
    the_descriptor = descriptor

    if the_descriptor is None:
        the_descriptor = {}

    if isinstance(the_descriptor, six.string_types):
        try:
            if os.path.isfile(the_descriptor):
                with open(the_descriptor, 'r') as f:
                    the_descriptor = json.load(f)
            else:
                req = requests.get(the_descriptor)
                req.raise_for_status()
                # Force UTF8 encoding for 'text/plain' sources
                req.encoding = 'utf8'
                the_descriptor = req.json()
        except (IOError, requests.exceptions.RequestException) as error:
            message = 'Unable to load JSON at "%s"' % descriptor
            six.raise_from(exceptions.DataPackageException(message), error)
        except ValueError as error:
            # Python2 doesn't have json.JSONDecodeError (use ValueErorr)
            message = 'Unable to parse JSON at "%s". %s' % (descriptor, error)
            six.raise_from(exceptions.DataPackageException(message), error)

    if hasattr(the_descriptor, 'read'):
        try:
            the_descriptor = json.load(the_descriptor)
        except ValueError as e:
            six.raise_from(exceptions.DataPackageException(str(e)), e)

    if not isinstance(the_descriptor, dict):
        msg = 'Data must be a \'dict\', but was a \'{0}\''
        raise exceptions.DataPackageException(msg.format(type(the_descriptor).__name__))

    return the_descriptor


# Dereference descriptor

def dereference_package_descriptor(descriptor, base_path):
    """Dereference data package descriptor (IN-PLACE FOR NOW).
    """
    for resource in descriptor.get('resources', []):
        dereference_resource_descriptor(resource, base_path, descriptor)
    return descriptor


def dereference_resource_descriptor(descriptor, base_path, base_descriptor=None):
    """Dereference resource descriptor (IN-PLACE FOR NOW).
    """
    PROPERTIES = ['schema', 'dialect']
    if base_descriptor is None:
        base_descriptor = descriptor
    for property in PROPERTIES:
        value = descriptor.get(property)

        # URI -> No
        if not isinstance(value, six.string_types):
            continue

        # URI -> Pointer
        if value.startswith('#'):
            try:
                pointer = jsonpointer.JsonPointer(value[1:])
                descriptor[property] = pointer.resolve(base_descriptor)
            except Exception as error:
                message = 'Not resolved Pointer URI "%s" for resource.%s' % (value, property)
                six.raise_from(
                    exceptions.DataPackageException(message),
                    error
                )

        # URI -> Remote
        elif base_path.startswith('http') or value.startswith('http'):
            try:
                fullpath = value
                if not value.startswith('http'):
                    fullpath = os.path.join(base_path, value)
                response = requests.get(fullpath)
                response.raise_for_status()
                descriptor[property] = response.json()
            except Exception as error:
                message = 'Not resolved Remote URI "%s" for resource.%s' % (value, property)
                six.raise_from(
                    exceptions.DataPackageException(message),
                    error
                )

        # URI -> Local
        else:
            if not is_safe_path(value):
                raise exceptions.DataPackageException(
                    'Not safe path in Local URI "%s" '
                    'for resource.%s' % (value, property))
            if not base_path:
                raise exceptions.DataPackageException(
                    'Local URI "%s" requires base path '
                    'for resource.%s' % (value, property))
            fullpath = os.path.join(base_path, value)
            try:
                with io.open(fullpath, encoding='utf-8') as file:
                    descriptor[property] = json.load(file)
            except Exception as error:
                message = 'Not resolved Local URI "%s" for resource.%s' % (value, property)
                six.raise_from(
                    exceptions.DataPackageException(message),
                    error
                )

    return descriptor


# Expand descriptor

def expand_package_descriptor(descriptor):
    """Apply defaults to data package descriptor (IN-PLACE FOR NOW).
    """
    descriptor.setdefault('profile', config.DEFAULT_DATA_PACKAGE_PROFILE)
    for resource in descriptor.get('resources', []):
        expand_resource_descriptor(resource)
    return descriptor


def expand_resource_descriptor(descriptor):
    """Apply defaults to resource descriptor (IN-PLACE FOR NOW).
    """
    descriptor.setdefault('profile', config.DEFAULT_RESOURCE_PROFILE)
    if descriptor['profile'] == 'tabular-data-resource':

        # Schema
        schema = descriptor.get('schema')
        if schema is not None:
            for field in schema.get('fields', []):
                field.setdefault('type', config.DEFAULT_FIELD_TYPE)
                field.setdefault('format', config.DEFAULT_FIELD_FORMAT)
            schema.setdefault('missingValues', config.DEFAULT_MISSING_VALUES)

        # Dialect
        dialect = descriptor.get('dialect')
        if dialect is not None:
            for key, value in config.DEFAULT_DIALECT.items():
                dialect.setdefault(key, value)

    return descriptor


# Miscellaneous

def ensure_dir(path):
    """Ensure directory exists.
    """
    dirpath = os.path.dirname(path)
    if dirpath and not os.path.exists(dirpath):
        os.makedirs(dirpath)


def is_safe_path(path):
    """Check if path is safe and allowed.
    """
    contains_windows_var = lambda val: re.match(r'%.+%', val)
    contains_posix_var = lambda val: re.match(r'\$.+', val)

    unsafeness_conditions = [
        os.path.isabs(path),
        ('..%s' % os.path.sep) in path,
        path.startswith('~'),
        os.path.expandvars(path) != path,
        contains_windows_var(path),
        contains_posix_var(path),
    ]

    return not any(unsafeness_conditions)


def extract_sha256_hash(hash):
    """Extrach SHA256 hash or return None
    """
    prefix = 'sha256:'
    if hash and hash.startswith(prefix):
        return hash.replace(prefix, '')
    return None
