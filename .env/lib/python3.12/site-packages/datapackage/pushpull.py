# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import os
import re
import six
import json
import warnings
import unicodecsv as csv
from copy import deepcopy
from importlib import import_module
from tableschema import Schema
from .package import Package
from . import helpers


# Module API

def push_datapackage(descriptor, backend, **backend_options):
    # Push Data Package to storage.

    # Deprecated
    warnings.warn(
        'Functions "push/pull_datapackage" are deprecated. '
        'Please use "Package" class',
        UserWarning)

    # Init maps
    tables = []
    schemas = []
    datamap = {}
    mapping = {}

    # Init model
    model = Package(descriptor)

    # Get storage
    plugin = import_module('jsontableschema.plugins.%s' % backend)
    storage = plugin.Storage(**backend_options)

    # Collect tables/schemas/data
    for resource in model.resources:
        if not resource.tabular:
            continue
        name = resource.descriptor.get('name', None)
        table = _convert_path(resource.descriptor['path'], name)
        schema = resource.descriptor['schema']
        data = resource.table.iter(keyed=True)
        # TODO: review
        def values(schema, data):
            for item in data:
                row = []
                for field in schema['fields']:
                    row.append(item.get(field['name'], None))
                yield tuple(row)
        tables.append(table)
        schemas.append(schema)
        datamap[table] = values(schema, data)
        if name is not None:
            mapping[name] = table
    schemas = _convert_schemas(mapping, schemas)

    # Create tables
    for table in tables:
        if table in storage.buckets:
            storage.delete(table)
    storage.create(tables, schemas)

    # Write data to tables
    for table in storage.buckets:
        if table in datamap:
            storage.write(table, datamap[table])
    return storage


def pull_datapackage(descriptor, name, backend, **backend_options):
    # Pull Data Package from storage.

    # Deprecated
    warnings.warn(
        'Functions "push/pull_datapackage" are deprecated. '
        'Please use "Package" class',
        UserWarning)

    # Save datapackage name
    datapackage_name = name

    # Get storage
    plugin = import_module('jsontableschema.plugins.%s' % backend)
    storage = plugin.Storage(**backend_options)

    # Iterate over tables
    resources = []
    for table in storage.buckets:

        # Prepare
        schema = storage.describe(table)
        base = os.path.dirname(descriptor)
        path, name = _restore_path(table)
        fullpath = os.path.join(base, path)

        # Write data
        helpers.ensure_dir(fullpath)
        with io.open(fullpath, 'wb') as file:
            model = Schema(deepcopy(schema))
            data = storage.iter(table)
            writer = csv.writer(file, encoding='utf-8')
            writer.writerow(model.headers)
            for row in data:
                writer.writerow(row)

        # Add resource
        resource = {'schema': schema, 'path': path}
        if name is not None:
            resource['name'] = name
        resources.append(resource)

    # Write descriptor
    mode = 'w'
    encoding = 'utf-8'
    if six.PY2:
        mode = 'wb'
        encoding = None
    resources = _restore_resources(resources)
    helpers.ensure_dir(descriptor)
    with io.open(descriptor,
                 mode=mode,
                 encoding=encoding) as file:
        descriptor = {
            'name': datapackage_name,
            'resources': resources,
        }
        json.dump(descriptor, file, indent=4)
    return storage


# Internal

def _convert_path(path, name):
    """Convert resource's path and name to storage's table name.

    Args:
        path (str): resource path
        name (str): resource name

    Returns:
        str: table name

    """
    table = os.path.splitext(path)[0]
    table = table.replace(os.path.sep, '__')
    if name is not None:
        table = '___'.join([table, name])
    table = re.sub('[^0-9a-zA-Z_]+', '_', table)
    table = table.lower()
    return table


def _restore_path(table):
    """Restore resource's path and name from storage's table.

    Args:
        table (str): table name

    Returns:
        (str, str): resource path and name

    """
    name = None
    splited = table.split('___')
    path = splited[0]
    if len(splited) == 2:
        name = splited[1]
    path = path.replace('__', os.path.sep)
    path += '.csv'
    return path, name


def _convert_schemas(mapping, schemas):
    """Convert schemas to be compatible with storage schemas.

    Foreign keys related operations.

    Args:
        mapping (dict): mapping between resource name and table name
        schemas (list): schemas

    Raises:
        ValueError: if there is no resource
            for some foreign key in given mapping

    Returns:
        list: converted schemas

    """
    schemas = deepcopy(schemas)
    for schema in schemas:
        for fk in schema.get('foreignKeys', []):
            resource = fk['reference']['resource']
            if resource != 'self':
                if resource not in mapping:
                    message = 'Not resource "%s" for foreign key "%s"'
                    message = message % (resource, fk)
                    raise ValueError(message)
                fk['reference']['resource'] = mapping[resource]
    return schemas


def _restore_resources(resources):
    """Restore schemas from being compatible with storage schemas.

    Foreign keys related operations.

    Args:
        list: resources from storage

    Returns:
        list: restored resources

    """
    resources = deepcopy(resources)
    for resource in resources:
        schema = resource['schema']
        for fk in schema.get('foreignKeys', []):
            _, name = _restore_path(fk['reference']['resource'])
            fk['reference']['resource'] = name
    return resources
