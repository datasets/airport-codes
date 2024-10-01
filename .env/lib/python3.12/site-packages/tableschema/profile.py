# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import io
import os
import six
import json
import jsonschema
from jsonschema.validators import validator_for
from . import exceptions


# Module API

class Profile(object):

    # Public

    def __init__(self, profile):
        self.__profile = profile
        self.__jsonschema = _PROFILES.get(profile)
        if not self.__jsonschema:
            message = 'Can\'t load profile "%s"' % profile
            raise exceptions.LoadError(message)

    @property
    def name(self):
        return self.__jsonschema.get('title', '').replace(' ', '-').lower() or None

    @property
    def jsonschema(self):
        return self.__jsonschema

    def validate(self, descriptor):

        # Other profiles
        if self.name != 'table-schema':
            return jsonschema.validate(descriptor, self.jsonschema)

        # Collect errors
        errors = []
        validator = _TableSchemaValidator(
            self.jsonschema, format_checker=jsonschema.FormatChecker())
        for error in validator.iter_errors(descriptor):
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

        # Railse error
        if errors:
            message = 'There are %s validation errors (see exception.errors)' % len(errors)
            raise exceptions.ValidationError(message, errors=errors)

        return True


# Internal

def _load_profile(filename):
    path = os.path.join(os.path.dirname(__file__), 'profiles', filename)
    profile = json.load(io.open(path, encoding='utf-8'))
    return profile


_PROFILES = {
    'table-schema': _load_profile('table-schema.json'),
    'geojson': _load_profile('geojson.json'),
}

_CONSTRAINT_ALLOWED_FIELD_TYPE = {
    'minLength': {None, 'string', 'array', 'object'},
    'maxLength': {None, 'string', 'array', 'object'},
    'minimum': {'integer', 'number', 'date', 'time', 'datetime', 'year', 'yearmonth'},
    'maximum': {'integer', 'number', 'date', 'time', 'datetime', 'year', 'yearmonth'},
    'pattern': {None, 'string'},
}


class _TableSchemaValidator(validator_for(_PROFILES['table-schema'])):
    @classmethod
    def check_schema(cls, schema):
        # When checking against the metaschema, we do not want to run the
        # additional checking added in iter_errors
        parent_cls = cls.__bases__[0]
        for error in parent_cls(cls.META_SCHEMA).iter_errors(schema):
            raise jsonschema.exceptions.SchemaError.create_from(error)

    def iter_errors(self, instance, _schema=None):

        # iter jsonschema validation errors
        for error in super(_TableSchemaValidator, self).iter_errors(instance, _schema):
            yield jsonschema.exceptions.ValidationError(
                error.message, error.validator, error.path, error.cause,
                error.context, error.validator_value, error.instance,
                error.schema, error.schema_path, error.parent)

        # get field names
        try:
            field_names = [f['name'] for f in instance['fields']]
        except (TypeError, KeyError):
            field_names = []

        # ensure constraint and field type consistency
        if isinstance(instance, dict) and instance.get('fields'):
            for field in instance['fields']:
                if not isinstance(field, dict):
                    continue
                field_type = field.get('type')
                field_type_str = 'default' if field_type is None else field_type
                field_name = field.get('name', '[noname]')
                constraints = field.get('constraints', {})
                for constraint_name in constraints:
                    if constraint_name in _CONSTRAINT_ALLOWED_FIELD_TYPE:
                        if field_type not in _CONSTRAINT_ALLOWED_FIELD_TYPE[constraint_name]:
                            yield exceptions.ValidationError(
                                "field {}: built-in {} constraint can't be applied to {} type field"
                                .format(field_name, constraint_name, field_type_str))

        # the hash MAY contain a key `primaryKey`
        if isinstance(instance, dict) and instance.get('primaryKey'):

            # ensure that the primary key matches field names
            if isinstance(instance['primaryKey'], six.string_types):
                if not instance['primaryKey'] in field_names:
                    yield exceptions.ValidationError(
                        'A JSON Table Schema primaryKey value must be found in'
                        ' the schema field names')
            elif isinstance(instance['primaryKey'], list):
                for k in instance['primaryKey']:
                    if k not in field_names:
                        yield exceptions.ValidationError(
                            'A JSON Table Schema primaryKey value must be '
                            'found in the schema field names')

        # the hash may contain a key `foreignKeys`
        if isinstance(instance, dict) and instance.get('foreignKeys'):
            for fk in instance['foreignKeys']:

                # ensure that `foreignKey.fields` match field names
                if isinstance(fk.get('fields'), six.string_types):
                    if fk.get('fields') not in field_names:
                        yield exceptions.ValidationError(
                            'A JSON Table Schema foreignKey.fields value must '
                            'correspond with field names.')
                elif isinstance(fk.get('fields', []), list):
                    for field in fk.get('fields'):
                        if field not in field_names:
                            yield exceptions.ValidationError(
                                'A JSON Table Schema foreignKey.fields value '
                                'must correspond with field names.')

                # ensure that `foreignKey.reference.fields`
                # matches outer `fields`
                if isinstance(fk.get('fields'), six.string_types):
                    fields = fk.get('reference', {}).get('fields', {})
                    if not isinstance(fields, six.string_types):
                        yield exceptions.ValidationError(
                            'A JSON Table Schema foreignKey.reference.fields '
                            'must match field names.')
                else:
                    if isinstance(fk['reference']['fields'], six.string_types):
                        yield exceptions.ValidationError(
                            'A JSON Table Schema foreignKey.fields cannot '
                            'be a string when foreignKey.reference.fields.'
                            'is a string')
                    if not (len(fk.get('fields')) ==
                            len(fk['reference']['fields'])):
                        yield exceptions.ValidationError(
                            'A JSON Table Schema foreignKey.fields must '
                            'contain the same number entries as '
                            'foreignKey.reference.fields.')
