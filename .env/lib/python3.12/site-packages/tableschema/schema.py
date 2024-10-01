# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import io
import six
import json
from collections import OrderedDict
from copy import deepcopy
from six.moves import zip_longest
from .profile import Profile
from .field import Field
from . import exceptions
from . import helpers
from . import config
from . import types


# Module API

class Schema(object):
    """Schema representation

    # Arguments
        descriptor (str/dict): schema descriptor one of:
            - local path
            - remote url
            - dictionary
        strict (bool): flag to specify validation behaviour:
            - if false, errors will not be raised but instead collected in `schema.errors`
            - if true, validation errors are raised immediately

    # Raises
        TableSchemaException: raise any error that occurs during the process

    """

    # Public

    def __init__(self, descriptor={}, strict=False):

        # Process descriptor
        descriptor = helpers.retrieve_descriptor(descriptor)

        # Set attributes
        self.__strict = strict
        self.__current_descriptor = deepcopy(descriptor)
        self.__next_descriptor = deepcopy(descriptor)
        self.__profile = Profile('table-schema')
        self.__errors = []
        self.__fields = []

        # Build instance
        self.__build()

    @property
    def valid(self):
        """Validation status

        Always true in strict mode.

        # Returns
            bool: validation status

        """
        return not bool(self.__errors)

    @property
    def errors(self):
        """Validation errors

        Always empty in strict mode.

        # Returns
            Exception[]: validation errors

        """
        return self.__errors

    @property
    def descriptor(self):
        """Schema's descriptor

        # Returns
            dict: descriptor

        """
        # Never use this.descriptor inside this class (!!!)
        return self.__next_descriptor

    @property
    def missing_values(self):
        """Schema's missing values

        # Returns
            str[]: missing values

        """
        return self.__current_descriptor.get('missingValues', [])

    @property
    def primary_key(self):
        """Schema's primary keys

        # Returns
            str[]: primary keys

        """
        primary_key = self.__current_descriptor.get('primaryKey', [])
        if not isinstance(primary_key, list):
            primary_key = [primary_key]
        return primary_key

    @property
    def foreign_keys(self):
        """Schema's foreign keys

        # Returns
            dict[]: foreign keys

        """
        foreign_keys = self.__current_descriptor.get('foreignKeys', [])
        for key in foreign_keys:
            key.setdefault('fields', [])
            key.setdefault('reference', {})
            key['reference'].setdefault('resource', '')
            key['reference'].setdefault('fields', [])
            if not isinstance(key['fields'], list):
                key['fields'] = [key['fields']]
            if not isinstance(key['reference']['fields'], list):
                key['reference']['fields'] = [key['reference']['fields']]
        return foreign_keys

    @property
    def fields(self):
        """Schema's fields

        # Returns
            Field[]: an array of field instances

        """
        return self.__fields

    @property
    def field_names(self):
        """Schema's field names

        # Returns
            str[]: an array of field names

        """
        return [field.name for field in self.fields]

    def get_field(self, name):
        """Get schema's field by name.

        > Use `table.update_field` if you want to modify the field descriptor

        # Arguments
            name (str): schema field name

        # Returns
           Field/None: `Field` instance or `None` if not found

        """
        for field in self.fields:
            if field.name == name:
                return field
        return None

    def add_field(self, descriptor):
        """ Add new field to schema.

        The schema descriptor will be validated with newly added field descriptor.

        # Arguments
            descriptor (dict): field descriptor

        # Raises
            TableSchemaException: raises any error that occurs during the process

        # Returns
            Field/None: added `Field` instance or `None` if not added

        """
        self.__current_descriptor.setdefault('fields', [])
        self.__current_descriptor['fields'].append(descriptor)
        self.__build()
        return self.__fields[-1]

    def update_field(self, name, update):
        """Update existing descriptor field by name

        # Arguments
            name (str): schema field name
            update (dict): update to apply to field's descriptor

        # Returns
            bool: true on success and false if no field is found to be modified

        """
        for field in self.__next_descriptor['fields']:
            if field['name'] == name:
                field.update(update)
                return True
        return False

    def remove_field(self, name):
        """Remove field resource by name.

        The schema descriptor will be validated after field descriptor removal.

        # Arguments
            name (str): schema field name

        # Raises
            TableSchemaException: raises any error that occurs during the process

        # Returns
            Field/None: removed `Field` instances or `None` if not found

        """
        field = self.get_field(name)
        if field:
            predicat = lambda field: field.get('name') != name
            self.__current_descriptor['fields'] = list(filter(
                predicat, self.__current_descriptor['fields']))
            self.__build()
        return field

    def cast_row(self, row, fail_fast=False, row_number=None, exc_handler=None):
        """Cast row based on field types and formats.

        # Arguments
            row (any[]: data row as an array of values

        # Returns
            any[]: returns cast data row

        """
        exc_handler = helpers.default_exc_handler if exc_handler is None else \
            exc_handler

        # Prepare
        result = []
        errors = []
        if row_number is not None:
            row_number_info = ' for row "%s"' % row_number
        else:
            row_number_info = ''
        # Check row length
        if len(row) != len(self.fields):
            message = (
                'Row length %s doesn\'t match fields count %s' +
                row_number_info) % (len(row), len(self.fields))
            exc = exceptions.CastError(message)
            # Some preparations for error reporting, relevant if custom error
            # handling is in place.
            if len(row) < len(self.fields):
                # Treat missing col values as None
                keyed_row = OrderedDict(
                    zip_longest((field.name for field in self.fields), row))
                # Use added None values for further processing
                row = list(keyed_row.values())
            else:
                fields = self.fields
                keyed_row = OrderedDict(
                    # Use extra column number if value index exceeds fields
                    (fields[i].name if fields[i:]
                     else 'tableschema-cast-error-extra-col-{}'.format(i+1),
                     value)
                    for (i, value) in enumerate(row))
            exc_handler(exc, row_number=row_number, row_data=keyed_row,
                        error_data=keyed_row)

        # Cast row
        for field, value in zip(self.fields, row):
            try:
                result.append(field.cast_value(value))
            except exceptions.CastError as exception:
                if fail_fast:
                    raise
                # Wrap original value in a FailedCast object to be able to
                # further process/yield values and to distinguish uncasted
                # values on the consuming side.
                result.append(FailedCast(value))
                errors.append(exception)

        # Raise errors
        if errors:
            message = (
                'There are %s cast errors (see exception.errors)' +
                row_number_info) % len(errors)
            keyed_row = OrderedDict(zip(self.field_names, row))
            # Add the cast failure-causing fields only to error data.
            # Indexing results with the row field index should be ok at this
            # point due to the previous processing.
            error_data = OrderedDict(
                (name, value)
                for (i, (name, value)) in enumerate(keyed_row.items())
                if isinstance(result[i], FailedCast))
            exc_handler(
                exceptions.CastError(message, errors=errors),
                row_number=row_number, row_data=keyed_row,
                error_data=error_data)

        return result

    def infer(self, rows, headers=1, confidence=0.75,
              guesser_cls=None, resolver_cls=None):
        """Infer and set `schema.descriptor` based on data sample.

        # Arguments
            rows (list[]): array of arrays representing rows.
            headers (int/str[]): data sample headers (one of):
              - row number containing headers (`rows` should contain headers rows)
              - array of headers (`rows` should NOT contain headers rows)
            confidence (float): how many casting errors are allowed (as a ratio, between 0 and 1)
            guesser_cls (class): you can implement inferring strategies by
                 providing type-guessing and type-resolving classes [experimental]
            resolver_cls (class): you can implement inferring strategies by
                 providing type-guessing and type-resolving classes [experimental]

        # Returns
            dict: Table Schema descriptor

        """

        # Get headers
        if isinstance(headers, int):
            headers_row = headers
            while True:
                headers_row -= 1
                headers = rows.pop(0)
                if not headers_row:
                    break
        elif isinstance(headers, list):
            seen_cells = []
            headers = list(headers)
            for index, cell in enumerate(headers):
                count = seen_cells.count(cell) + 1
                headers[index] = '%s%s' % (cell, count) if count > 1 else cell
                seen_cells.append(cell)
        elif not isinstance(headers, list):
            headers = []

        # Get descriptor
        missing_values = self.__current_descriptor.get('missingValues', config.DEFAULT_MISSING_VALUES)
        guesser = guesser_cls() if guesser_cls else _TypeGuesser(missing_values)
        resolver = (resolver_cls or _TypeResolver)()
        descriptor = {'fields': [], 'missingValues': missing_values}
        type_matches = {}
        for number, header in enumerate(headers, start=1):
            descriptor['fields'].append({'name': header or 'field%s' % number})
        for index, row in enumerate(rows):
            # Normalize rows with invalid dimensions for sanity
            row_length = len(row)
            headers_length = len(headers)
            if row_length > headers_length:
                row = row[:len(headers)]
            if row_length < headers_length:
                diff = headers_length - row_length
                fill = [''] * diff
                row = row + fill
            # build a column-wise lookup of type matches
            for index, value in enumerate(row):
                rv = guesser.cast(value)
                if type_matches.get(index):
                    type_matches[index].extend(rv)
                else:
                    type_matches[index] = list(rv)
        # choose a type/format for each column based on the matches
        for index, results in type_matches.items():
            rv = resolver.get(results, confidence)
            descriptor['fields'][index].update(**rv)

        # Save descriptor
        self.__current_descriptor = descriptor
        self.__build()

        return descriptor

    def commit(self, strict=None):
        """Update schema instance if there are in-place changes in the descriptor.

        # Example

        ```python
        from tableschema import Schema
        descriptor = {'fields': [{'name': 'my_field', 'title': 'My Field', 'type': 'string'}]}
        schema = Schema(descriptor)
        print(schema.get_field('my_field').descriptor['type']) # string

        # Update descriptor by field position
        schema.descriptor['fields'][0]['type'] = 'number'
        # Update descriptor by field name
        schema.update_field('my_field', {'title': 'My Pretty Field'}) # True

        # Change are not committed
        print(schema.get_field('my_field').descriptor['type']) # string
        print(schema.get_field('my_field').descriptor['title']) # My Field

        # Commit change
        schema.commit()
        print(schema.get_field('my_field').descriptor['type']) # number
        print(schema.get_field('my_field').descriptor['title']) # My Pretty Field

        ```

        # Arguments
            strict (bool): alter `strict` mode for further work

        # Raises
            TableSchemaException: raises any error that occurs during the process

        # Returns
            bool: true on success and false if not modified

        """
        if strict is not None:
            self.__strict = strict
        elif self.__current_descriptor == self.__next_descriptor:
            return False
        self.__current_descriptor = deepcopy(self.__next_descriptor)
        self.__build()
        return True

    def save(self, target, ensure_ascii=True):
        """Save schema descriptor to target destination.

        # Arguments
            target (str): path where to save a descriptor

        # Raises
            TableSchemaException: raises any error that occurs during the process

        # Returns
            bool: true on success

        """
        mode = 'w'
        encoding = 'utf-8'
        if six.PY2:
            mode = 'wb'
            encoding = None
        helpers.ensure_dir(target)
        with io.open(target, mode=mode, encoding=encoding) as file:
            json.dump(self.__current_descriptor, file, indent=4, ensure_ascii=ensure_ascii)

    # Internal

    def __build(self):

        # Process descriptor
        expand = helpers.expand_schema_descriptor
        self.__current_descriptor = expand(self.__current_descriptor)
        self.__next_descriptor = deepcopy(self.__current_descriptor)

        # Validate descriptor
        try:
            self.__profile.validate(self.__current_descriptor)
            self.__errors = []
        except exceptions.ValidationError as exception:
            self.__errors = exception.errors
            if self.__strict:
                raise exception

        # Populate fields
        self.__fields = []
        for field in self.__current_descriptor.get('fields', []):
            missing_values = self.__current_descriptor['missingValues']
            try:
                field = Field(field, missing_values=missing_values, schema=self)
            except exceptions.TableSchemaException as e:
                if self.__strict:
                    raise e
                else:
                    field = False
            self.__fields.append(field)

    # Deprecated

    headers = field_names
    has_field = get_field


class FailedCast(object):
    """Wrap an original data field value that failed to be properly casted.

    FailedCast allows for further processing/yielding values but still be able
    to distinguish uncasted values on the consuming side.

    Delegates attribute access and the basic rich comparison methods to the
    underlying object. Supports default user-defined classes hashability i.e.
    is hashable based on object identity (not based on the wrapped value).

    # Arguments
        value (any): value

    """

    # Make this "reasonably immutable": Don't support setting other attributes,
    # don't support modifying re-setting value
    __slots__ = ('_value',)

    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

    def __repr__(self):
        return 'FailedCast(%r)' % self._value

    def __getattr__(self, name):
        return getattr(self._value, name)

    def __lt__(self, other):
        return self._value < other

    def __le__(self, other):
        return self._value <= other

    def __eq__(self, other):
        return self._value == other

    def __ne__(self, other):
        return self._value != other

    def __gt__(self, other):
        return self._value > other

    def __ge__(self, other):
        return self._value >= other

    def __hash__(self):
        return object.__hash__(self)


# Internal
_INFER_DATE_FORMATS = [
    '%Y-%m-%d',
    '%d/%m/%Y',
    '%m/%d/%Y',
    '%d/%m/%y',
    '%m/%d/%y',
    '%Y%m%d',
    '%d-%m-%y',
    '%Y/%m/%d',
    '%d.%m.%Y',
    '%d%m%y',
    '%d.%m.%y',
]


_INFER_TYPE_ORDER = [
    'duration',
    'geojson',
    'geopoint',
    'object',
    'array',
    'datetime',
    'time',
    ('date', _INFER_DATE_FORMATS),
    'integer',
    'number',
    'boolean',
    'string',
    'any',
]


class _TypeGuesser(object):
    """Guess the type for a value returning a tuple of ('type', 'format')
    """

    # Public

    def __init__(self, missing_values):
        self.missing_values = missing_values

    def cast(self, value):
        for priority, type_rec in enumerate(_INFER_TYPE_ORDER):
            if isinstance(type_rec, tuple):
                name, formats = type_rec
            else:
                name, formats = type_rec, ['default']
            cast = getattr(types, 'cast_%s' % name)
            if value not in self.missing_values:
                for format in formats:
                    result = cast(format, value)
                    if result != config.ERROR:
                        yield (name, format, priority)


class _TypeResolver(object):
    """Get the best matching type/format from a list of possible ones.
    """

    # Public

    def get(self, results, confidence):
        variants = set(results)
        # only one candidate... that's easy.
        if len(variants) == 1:
            rv = {'type': results[0][0], 'format': results[0][1]}
        else:
            counts = {}
            for result in results:
                if counts.get(result):
                    counts[result] += 1
                else:
                    counts[result] = 1
            # tuple representation of `counts` dict sorted by values
            sorted_counts = sorted(counts.items(), key=lambda item: item[1], reverse=True)
            if not sorted_counts:
                return {'type': 'string', 'format': 'default'}
            # Allow also counts that are not the max, based on the confidence
            max_count = sorted_counts[0][1]
            sorted_counts = filter(lambda item: item[1] >= max_count * confidence,
                                   sorted_counts)
            # Choose the most specific data type
            sorted_counts = sorted(sorted_counts,
                                   key=lambda item: item[0][2])
            rv = {'type': sorted_counts[0][0][0], 'format': sorted_counts[0][0][1]}
        return rv
