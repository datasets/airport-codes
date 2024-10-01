from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import six
import copy
import warnings
import requests
import jsonschema
import datapackage.registry
from . import exceptions


# Module API

class Profile(object):
    """Profile representation

    # Arguments
        profile (str): profile name in registry or URL to JSON Schema

    # Raises
        DataPackageException: raises error if something goes wrong

    """

    # Public

    def __init__(self, profile):
        self._name = profile
        self._registry = self._load_registry()
        self._schema = self._load_schema(profile, self._registry)
        self._validator = self._load_validator(self._schema, self._registry)
        self._check_schema()

    @property
    def name(self):
        """Profile name

        # Returns
            str/None: name if available

        """
        return self._name

    @property
    def jsonschema(self):
        """JSONSchema content

        # Returns
            dict: returns profile's JSON Schema contents

        """
        return self._schema

    def validate(self, descriptor):
        """Validate a data package `descriptor` against the profile.

        # Arguments
            descriptor (dict): retrieved and dereferenced data package descriptor

        # Raises
            ValidationError: raises if not valid
        # Returns
            bool: returns True if valid

        """

        # Collect errors
        errors = []
        for error in self._validator.iter_errors(descriptor):
            if isinstance(error, jsonschema.exceptions.ValidationError):
                message = str(error.message)
                if six.PY2:
                    message = message.replace('u\'', '\'')
                descriptor_path = '/'.join(map(str, error.path))
                profile_path = '/'.join(map(str, error.schema_path))
                error = exceptions.ValidationError(
                    'Descriptor validation error: %s '
                    'at "%s" in descriptor and '
                    'at "%s" in profile'
                    % (message, descriptor_path, profile_path))
            errors.append(error)

        # Raise error
        if errors:
            message = 'There are %s validation errors (see exception.errors)' % len(errors)
            raise exceptions.ValidationError(message, errors=errors)

        return True

    # Private

    def _load_registry(self):
        return datapackage.registry.Registry()

    def _load_schema(self, schema, registry):
        the_schema = schema

        # For a reference:
        # https://frictionlessdata.io/specs/profiles/
        if isinstance(schema, six.string_types):
            try:
                the_schema = registry.get(schema)
                if not the_schema:
                    req = requests.get(schema)
                    req.raise_for_status()
                    the_schema = req.json()
            except (IOError, ValueError, requests.exceptions.RequestException) as ex:
                message = 'Unable to load profile at "{0}"'
                six.raise_from(
                    exceptions.ValidationError(message.format(schema)),
                    ex
                )

        elif isinstance(the_schema, dict):
            the_schema = copy.deepcopy(the_schema)
        else:
            message = 'Schema must be a "dict", but was a "{0}"'
            raise exceptions.ValidationError(message.format(type(the_schema).__name__))

        return the_schema

    def _load_validator(self, schema, registry):
        validator_class = jsonschema.validators.validator_for(schema)
        return validator_class(schema)

    def _check_schema(self):
        try:
            self._validator.check_schema(self._schema)
        except jsonschema.exceptions.SchemaError as ex:
            six.raise_from(
                exceptions.ValidationError('Profile is invalid: %s' % ex),
                ex
            )

    def __getattr__(self, name):
        if name in self.__dict__.get('_schema', {}):
            return copy.deepcopy(self._schema[name])

        msg = '\'{0}\' object has no attribute \'{1}\''
        raise AttributeError(msg.format(self.__class__.__name__, name))

    def __setattr__(self, name, value):
        if name in self.__dict__.get('_schema', {}):
            raise AttributeError('can\'t set attribute')
        super(self.__class__, self).__setattr__(name, value)

    def __dir__(self):
        return list(self.__dict__.keys()) + list(self._schema.keys())

    # Deprecated

    def iter_errors(self, data):
        # Lazily yields each ValidationError for the received data dict.

        # Deprecate
        warnings.warn(
            'Property "profile.iter_errors" is deprecated.',
            UserWarning)

        for error in self._validator.iter_errors(data):
            yield error

    def to_dict(self):
        # dict: Convert this :class:`.Schema` to dict.

        # Deprecate
        warnings.warn(
            'Property "profile.to_dict" is deprecated.',
            UserWarning)

        return copy.deepcopy(self._schema)
