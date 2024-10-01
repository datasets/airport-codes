# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
from functools import partial
from cached_property import cached_property
from .profile import Profile
from . import constraints
from . import exceptions
from . import helpers
from . import config
from . import types


# Module API

class Field(object):
    """Field representaion

    # Arguments
        descriptor (dict): schema field descriptor
        missingValues (str[]): an array with string representing missing values

    # Raises
        TableSchemaException: raises any error that occurs during the process

    """

    # Public

    ERROR = config.ERROR

    def __init__(self, descriptor, missing_values=config.DEFAULT_MISSING_VALUES,
                 # Internal
                 schema=None):

        # Process descriptor
        descriptor = helpers.expand_field_descriptor(descriptor)

        # Set attributes
        self.__descriptor = descriptor
        self.__missing_values = missing_values
        self.__schema = schema
        self.__cast_function = self.__get_cast_function()
        self.__check_functions = self.__get_check_functions()
        self.__preserve_missing_values = os.environ.get('TABLESCHEMA_PRESERVE_MISSING_VALUES')

    @cached_property
    def schema(self):
        """Returns a schema instance if the field belongs to some schema

        # Returns
            Schema: field's schema

        """
        return self.__schema

    @cached_property
    def name(self):
        """Field name

        # Returns
            str: field name

        """
        return self.__descriptor.get('name')

    @cached_property
    def type(self):
        """Field type

        # Returns
            str: field type

        """
        return self.__descriptor.get('type')

    @cached_property
    def format(self):
        """Field format

        # Returns
            str: field format

        """
        return self.__descriptor.get('format')

    @cached_property
    def missing_values(self):
        """Field's missing values

        # Returns
            str[]: missing values

        """
        return self.__missing_values

    @cached_property
    def required(self):
        """Whether field is required

        # Returns
            bool: true if required

        """
        return self.constraints.get('required', False)

    @cached_property
    def constraints(self):
        """Field constraints

        # Returns
            dict: dict of field constraints

        """
        return self.__descriptor.get('constraints', {})

    @cached_property
    def descriptor(self):
        """Fields's descriptor

        # Returns
            dict: descriptor

        """
        return self.__descriptor

    @cached_property
    def cast_function(self):
        return self.__cast_function

    @cached_property
    def check_functions(self):
        return self.__check_functions

    def cast_value(self, value, constraints=True):
        """Cast given value according to the field type and format.

        # Arguments
            value (any): value to cast against field
            constraints (boll/str[]): gets constraints configuration
                - it could be set to true to disable constraint checks
                - it could be an Array of constraints to check e.g. ['minimum', 'maximum']

        # Raises
            TableSchemaException: raises any error that occurs during the process

        # Returns
            any: returns cast value

        """

        # Null value
        if value in self.__missing_values:
            # Whether missing_values should be preserved without being cast
            if self.__preserve_missing_values:
                return value
            value = None

        # Cast value
        cast_value = value
        if value is not None:
            cast_value = self.__cast_function(value)
            if cast_value == config.ERROR:
                raise exceptions.CastError((
                    'Field "{field.name}" can\'t cast value "{value}" '
                    'for type "{field.type}" with format "{field.format}"'
                    ).format(field=self, value=value))

        # Check value
        if constraints:
            for name, check in self.__check_functions.items():
                if isinstance(constraints, list):
                    if name not in constraints:
                        continue
                passed = check(cast_value)
                if not passed:
                    raise exceptions.CastError((
                        'Field "{field.name}" has constraint "{name}" '
                        'which is not satisfied for value "{value}"'
                        ).format(field=self, name=name, value=value))

        return cast_value

    def test_value(self, value, constraints=True):
        """Test whether value is compliant to the field.

        # Arguments
            value (any): value to cast against field
            constraints (bool/str[]): constraints configuration

        # Returns
            bool: returns if value is compliant to the field

        """
        try:
            self.cast_value(value, constraints=constraints)
        except exceptions.CastError:
            return False
        return True

    # Private

    def __get_cast_function(self):
        options = {}
        # Get cast options
        for key in ['decimalChar', 'groupChar', 'bareNumber', 'trueValues', 'falseValues']:
            value = self.descriptor.get(key)
            if value is not None:
                options[key] = value
        try:
            cast = getattr(types, 'cast_%s' % self.type)
        except AttributeError:
            message = 'Not supported field type: %s' % self.type
            raise exceptions.TableSchemaException(message)
        cast = partial(cast, self.format, **options)
        return cast

    def __get_check_functions(self):
        checks = {}
        cast = partial(self.cast_value, constraints=False)
        whitelist = _get_field_constraints(self.type)
        for name, constraint in self.constraints.items():
            if name in whitelist:
                # Cast enum constraint
                if name in ['enum']:
                    constraint = list(map(cast, constraint))
                # Cast maximum/minimum constraint
                if name in ['maximum', 'minimum']:
                    constraint = cast(constraint)
                check = getattr(constraints, 'check_%s' % name)
                checks[name] = partial(check, constraint)
        return checks


# Internal

def _get_field_constraints(type):
    # Extract list of constraints for given type from jsonschema
    jsonschema = Profile('table-schema').jsonschema
    profile_types = jsonschema['properties']['fields']['items']['anyOf']
    for profile_type in profile_types:
        if type in profile_type['properties']['type']['enum']:
            return profile_type['properties']['constraints']['properties'].keys()
