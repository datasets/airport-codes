# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from . import config
__version__ = config.VERSION


# Module API

from .cli import cli
from .package import Package
from .resource import Resource
from .group import Group
from .profile import Profile
from .validate import validate
from .infer import infer
from .exceptions import DataPackageException
from .exceptions import TableSchemaException
from .exceptions import LoadError
from .exceptions import CastError
from .exceptions import IntegrityError
from .exceptions import RelationError
from .exceptions import StorageError


# Deprecated

from .pushpull import (push_datapackage,)
from .pushpull import (pull_datapackage,)
DataPackage = Package
