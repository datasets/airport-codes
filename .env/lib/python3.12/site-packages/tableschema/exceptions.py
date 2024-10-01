# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals


# Module API

class DataPackageException(Exception):
    """Base class for all DataPackage/TableSchema exceptions.

    If there are multiple errors, they can be read from the exception object:

    ```python
    try:
        # lib action
    except DataPackageException as exception:
        if exception.multiple:
            for error in exception.errors:
                # handle error
    ```

    """

    # Public

    def __init__(self, message, errors=None):
        self.__errors = errors or []
        super(Exception, self).__init__(message)

    @property
    def multiple(self):
        """Whether it's a nested exception

        # Returns
            bool: whether it's a nested exception

        """
        return bool(self.__errors)

    @property
    def errors(self):
        """List of nested errors

        # Returns
            DataPackageException[]: list of nested errors

        """
        return self.__errors


class TableSchemaException(DataPackageException):
    """Base class for all TableSchema exceptions.
    """
    pass


class LoadError(TableSchemaException):
    """All loading errors.
    """
    pass


class ValidationError(TableSchemaException):
    """All validation errors.
    """
    pass


class CastError(TableSchemaException):
    """All value cast errors.
    """
    pass


class IntegrityError(TableSchemaException):
    """All integrity errors.
    """
    pass


class UniqueKeyError(CastError):
    """Unique key constraint violation (CastError subclass)
    """
    pass


class RelationError(TableSchemaException):
    """All relations errors.
    """
    pass


class UnresolvedFKError(RelationError):
    """Unresolved foreign key reference error (RelationError subclass).
    """
    pass


class StorageError(TableSchemaException):
    """All storage errors.
    """
    pass


# Deprecated

MultipleInvalid = TableSchemaException
InvalidJSONError = LoadError
SchemaValidationError = ValidationError
InvalidSchemaError = ValidationError
InvalidCastError = CastError
ConstraintError = CastError
