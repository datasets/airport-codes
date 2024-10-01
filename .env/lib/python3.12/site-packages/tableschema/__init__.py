# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from . import config
__version__ = config.VERSION


# Module API

from .cli import cli
from .table import Table
from .schema import Schema
from .field import Field
from .storage import Storage
from .validate import validate
from .infer import infer
from .schema import FailedCast
from .exceptions import DataPackageException
from .exceptions import TableSchemaException
from .exceptions import LoadError
from .exceptions import ValidationError
from .exceptions import CastError
from .exceptions import IntegrityError
from .exceptions import UniqueKeyError
from .exceptions import RelationError
from .exceptions import UnresolvedFKError
from .exceptions import StorageError

# Deprecated

from . import exceptions
