# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from six import add_metaclass
from importlib import import_module
from abc import ABCMeta, abstractmethod
from . import exceptions


# Module API

@add_metaclass(ABCMeta)
class Storage(object):
    """Storage factory/interface

    # For users

    > Use `Storage.connect` to instantiate a storage

    For instantiation of concrete storage instances,
    `tableschema.Storage` provides a unified factory method `connect`
    (which uses the plugin system under the hood):

    ```python
    # pip install tableschema_sql
    from tableschema import Storage

    storage = Storage.connect('sql', **options)
    storage.create('bucket', descriptor)
    storage.write('bucket', rows)
    storage.read('bucket')
    ```

    # For integrators

    The library includes interface declaration to implement tabular `Storage`.
    This interface allow to use different data storage systems like SQL
    with `tableschema.Table` class (load/save) as well as on the data package level:

    ![Storage](https://raw.githubusercontent.com/frictionlessdata/tableschema-py/master/data/storage.png)

    An implementor must follow `tableschema.Storage` interface
    to write his own storage backend. Concrete storage backends
    could include additional functionality specific to conrete storage system.
    See `plugins` below to know how to integrate custom storage plugin into your workflow.

    """

    # Public

    @abstractmethod
    def __init__(self, **options):
        pass

    @classmethod
    def connect(cls, name, **options):
        """Create tabular `storage` based on storage name.

        > This method is statis: `Storage.connect()`

        # Arguments
            name (str): storage name like `sql`
            options (dict): concrete storage options

        # Raises
            StorageError: raises on any error

        # Returns
            Storage: returns `Storage` instance

        """
        if cls is not Storage:
            message = 'Storage.connect is not available on concrete implemetations'
            raise exceptions.StorageError(message)
        module = 'tableschema.plugins.%s' % name
        storage = import_module(module).Storage(**options)
        return storage

    @property
    @abstractmethod
    def buckets(self):
        """Return list of storage bucket names.

        A `bucket` is a special term which has almost the same meaning as `table`.
        You should consider `bucket` as a `table` stored in the `storage`.

        # Raises
            exceptions.StorageError: raises on any error

        # Returns
            str[]: return list of bucket names

        """
        pass

    @abstractmethod
    def create(self, bucket, descriptor, force=False):
        """Create one/multiple buckets.

        # Arguments
            bucket (str/list): bucket name or list of bucket names
            descriptor (dict/dict[]): schema descriptor or list of descriptors
            force (bool): whether to delete and re-create already existing buckets

        # Raises
            exceptions.StorageError: raises on any error

        """
        pass

    @abstractmethod
    def delete(self, bucket=None, ignore=False):
        """ Delete one/multiple/all buckets.

        # Arguments
            bucket (str/list/None): bucket name or list of bucket names to delete.
                If `None`, all buckets will be deleted
            descriptor (dict/dict[]): schema descriptor or list of descriptors
            ignore (bool): don't raise an error on non-existent bucket deletion

        # Raises
            exceptions.StorageError: raises on any error

        """
        pass

    @abstractmethod
    def describe(self, bucket, descriptor=None):
        """ Get/set bucket's Table Schema descriptor

        # Arguments
            bucket (str): bucket name
            descriptor (dict/None): schema descriptor to set

        # Raises
            exceptions.StorageError: raises on any error

        # Returns
            dict: returns Table Schema descriptor

        """
        pass

    @abstractmethod
    def iter(self, bucket):
        """Return an iterator of typed values based on the schema of this bucket.

        # Arguments
            bucket (str): bucket name

        # Raises
            exceptions.StorageError: raises on any error

        # Returns
            list[]: yields data rows

        """
        pass

    @abstractmethod
    def read(self, bucket):
        """Read typed values based on the schema of this bucket.

        # Arguments
            bucket (str): bucket name
        # Raises
            exceptions.StorageError: raises on any error
        # Returns
            list[]: returns data rows

        """
        pass

    @abstractmethod
    def write(self, bucket, rows):
        """ This method writes data rows into `storage`.

        It should store values of unsupported types as strings internally (like csv does).

        # Arguments
            bucket (str): bucket name
            rows (list[]): data rows to write

        # Raises
            exceptions.StorageError: raises on any error

        """
        pass
