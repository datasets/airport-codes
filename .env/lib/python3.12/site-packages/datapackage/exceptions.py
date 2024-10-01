from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import tableschema


# Module API

DataPackageException = tableschema.DataPackageException
TableSchemaException = tableschema.TableSchemaException
LoadError = tableschema.LoadError
ValidationError = tableschema.ValidationError
CastError = tableschema.CastError
IntegrityError = tableschema.IntegrityError
RelationError = tableschema.RelationError
StorageError = tableschema.StorageError

# We need these lines to generate documentation
DataPackageException.__module__ = 'datapackage.exceptions'
TableSchemaException.__module__ = 'datapackage.exceptions'
LoadError.__module__ = 'datapackage.exceptions'
ValidationError.__module__ = 'datapackage.exceptions'
CastError.__module__ = 'datapackage.exceptions'
IntegrityError.__module__ = 'datapackage.exceptions'
RelationError.__module__ = 'datapackage.exceptions'
StorageError.__module__ = 'datapackage.exceptions'


# Deprecated

class SchemaError(DataPackageException):
    pass


class RegistryError(DataPackageException):
    pass
