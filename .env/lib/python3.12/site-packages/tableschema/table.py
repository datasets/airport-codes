# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

from copy import copy
from tabulator import Stream
from functools import partial
from collections import OrderedDict
from .storage import Storage
from .schema import Schema
from . import exceptions
from . import helpers
from . import config
from collections import defaultdict


# Module API

class Table(object):
    """Table representation

    # Arguments
      source (str/list[]): data source one of:
        - local file (path)
        - remote file (url)
        - array of arrays representing the rows
      schema (any): data schema in all forms supported by `Schema` class
      strict (bool): strictness option to pass to `Schema` constructor
      post_cast (function[]): list of post cast processors
      storage (None): storage name like `sql` or `bigquery`
      options (dict): `tabulator` or storage's options

    # Raises
      TableSchemaException: raises on any error

    """

    # Public

    def __init__(self, source, schema=None, strict=False,
                 post_cast=[], storage=None, **options):

        # Set attributes
        self.__source = source
        self.__stream = None
        self.__schema = None
        self.__headers = None
        self.__storage = None
        self.__post_cast = copy(post_cast)

        # Schema
        if isinstance(schema, Schema):
            self.__schema = schema
        elif schema is not None:
            self.__schema = Schema(schema)

        # Stream (tabulator)
        if storage is None:
            options.setdefault('headers', 1)
            self.__stream = Stream(source,  **options)

        # Stream (storage)
        else:
            if not isinstance(storage, Storage):
                storage = Storage.connect(storage, **options)
            if self.__schema:
                storage.describe(source, self.__schema.descriptor)
            headers = Schema(storage.describe(source)).field_names
            self.__stream = Stream(partial(storage.iter, source), headers=headers)
            self.__storage = storage

    @property
    def headers(self):
        """Table's headers is available

        # Returns
            str[]: headers

        """
        return self.__headers

    @property
    def schema(self):
        """Returns schema class instance if available

        # Returns
            Schema: schema

        """
        return self.__schema

    @property
    def size(self):
        """Table's size in BYTES if it's available

        If it's already read using e.g. `table.read`, otherwise returns `None`.
        In the middle of an iteration it returns size of already read contents

        # Returns
            int/None: size in BYTES

        """
        if self.__stream:
            return self.__stream.size

    @property
    def hash(self):
        """Table's SHA256 hash if it's available.

        If it's already read using e.g. `table.read`, otherwise returns `None`.
        In the middle of an iteration it returns hash of already read contents

        # Returns
            str/None: SHA256 hash

        """
        if self.__stream:
            return self.__stream.hash

    def iter(self, keyed=False, extended=False, cast=True,
             integrity=False, relations=False,
             foreign_keys_values=False, exc_handler=None):
        """Iterates through the table data and emits rows cast based on table schema.

        # Arguments

            keyed (bool):
                yield keyed rows in a form of `{header1\\: value1, header2\\: value2}`
                (default is false; the form of rows is `[value1, value2]`)

            extended (bool):
                yield extended rows in a for of `[rowNumber, [header1, header2], [value1, value2]]`
                (default is false; the form of rows is `[value1, value2]`)

            cast (bool):
                disable data casting if false
                (default is true)

            integrity (dict):
                dictionary in a form of `{'size'\\: <bytes>, 'hash'\\: '<sha256>'}`
                to check integrity of the table when it's read completely.
                Both keys are optional.

            relations (dict):
                dictionary of foreign key references in a form
                of `{resource1\\: [{field1\\: value1, field2\\: value2}, ...], ...}`.
                If provided, foreign key fields will checked and resolved
                to one of their references (/!\\ one-to-many fk are not completely resolved).

            foreign_keys_values (dict):
                three-level dictionary of foreign key references optimized
                to speed up validation process in a form of
                `{resource1\\: {(fk_field1, fk_field2)\\: {(value1, value2)\\: {one_keyedrow}, ... }}}`.
                If not provided but relations is true, it will be created
                before the validation process by *index_foreign_keys_values* method

            exc_handler (func):
                optional custom exception handler callable.
                Can be used to defer raising errors (i.e. "fail late"), e.g.
                for data validation purposes. Must support the signature below

        # Custom exception handler

        ```python
        def exc_handler(exc, row_number=None, row_data=None, error_data=None):
            '''Custom exception handler (example)

            # Arguments:
                exc(Exception):
                    Deferred exception instance
                row_number(int):
                    Data row number that triggers exception exc
                row_data(OrderedDict):
                    Invalid data row source data
                error_data(OrderedDict):
                    Data row source data field subset responsible for the error, if
                    applicable (e.g. invalid primary or foreign key fields). May be
                    identical to row_data.
            '''
            # ...
        ```

        # Raises
            TableSchemaException: base class of any error
            CastError: data cast error
            IntegrityError: integrity checking error
            UniqueKeyError: unique key constraint violation
            UnresolvedFKError: unresolved foreign key reference error

        # Returns
            Iterator[list]: yields rows

        """
        exc_handler = helpers.default_exc_handler if exc_handler is None else \
            exc_handler

        # Prepare unique checks
        if cast:
            unique_fields_cache = {}
            if self.schema:
                unique_fields_cache = _create_unique_fields_cache(self.schema)
        # Prepare relation checks
        if relations and not foreign_keys_values:
            # we have to test relations but the index has not been precomputed
            # prepare the index to boost validation process
            foreign_keys_values = self.index_foreign_keys_values(relations)

        # Open/iterate stream
        with self.__stream as stream:
            iterator = stream.iter(extended=True)
            iterator = self.__apply_processors(
                iterator, cast=cast, exc_handler=exc_handler)
            for row_number, headers, row in iterator:

                # Get headers
                if not self.__headers:
                    self.__headers = headers

                # Check headers
                if cast:
                    if self.schema and self.headers:
                        if self.headers != self.schema.field_names:
                            message = (
                                'Table headers (%r) don\'t match '
                                'schema field names (%r) in row %s' % (
                                    self.headers, self.schema.field_names,
                                    row_number))
                            keyed_row = OrderedDict(zip(headers, row))
                            exc_handler(
                                exceptions.CastError(message),
                                row_number=row_number, row_data=keyed_row,
                                error_data=keyed_row)
                            continue

                # Check unique
                if cast:
                    for indexes, cache in unique_fields_cache.items():
                        keyed_values = OrderedDict(
                            (headers[i], value)
                            for i, value in enumerate(row) if i in indexes)
                        values = tuple(keyed_values.values())
                        if not all(map(lambda value: value is None, values)):
                            if values in cache['data']:
                                message = (
                                    'Field(s) "%s" duplicates in row "%s" '
                                    'for values %r' % (
                                        cache['name'], row_number, values))
                                exc_handler(
                                    exceptions.UniqueKeyError(message),
                                    row_number=row_number,
                                    row_data=OrderedDict(zip(headers, row)),
                                    error_data=keyed_values)
                            cache['data'].add(values)

                # Resolve relations
                if relations:
                    if self.schema:
                        row_with_relations = dict(zip(headers, copy(row)))
                        for foreign_key in self.schema.foreign_keys:
                            refValue = _resolve_relations(row, headers, foreign_keys_values,
                                                          foreign_key)
                            if refValue is None:
                                keyed_row = OrderedDict(zip(headers, row))
                                # local values of the FK
                                local_keyed_values = {
                                    key: keyed_row[key]
                                    for key in foreign_key['fields']
                                    }
                                local_values = tuple(local_keyed_values.values())
                                message = (
                                    'Foreign key "%s" violation in row "%s": '
                                    '%s not found in %s' % (
                                        foreign_key['fields'],
                                        row_number,
                                        local_values,
                                        foreign_key['reference']['resource']))
                                exc_handler(
                                    exceptions.UnresolvedFKError(message),
                                    row_number=row_number, row_data=keyed_row,
                                    error_data=local_keyed_values)
                                # If we reach this point we don't fail-early
                                # i.e. no exception has been raised. As the
                                # reference can't be resolved, use empty dict
                                # as the "unresolved result".
                                for field in foreign_key['fields']:
                                    if not isinstance(
                                            row_with_relations[field], dict):
                                        row_with_relations[field] = {}
                            elif type(refValue) is dict:
                                # Substitute resolved referenced object for
                                # original referencing field value.
                                # For a composite foreign key, this substitutes
                                # each part of the composite key with the
                                # referenced object.
                                for field in foreign_key['fields']:
                                    if type(row_with_relations[field]) is not dict:
                                        # no previous refValues injected on this field
                                        row_with_relations[field] = refValue
                                    else:
                                        # alreayd one ref, merging
                                        row_with_relations[field].update(refValue)
                            else:
                                # case when all original value of the FK are empty
                                # refValue == row, there is nothing to do
                                # an empty dict might be a better returned value for this case ?
                                pass

                        #  mutate row now that we are done, in the right order
                        row = [row_with_relations[f] for f in headers]

                # Form row
                if extended:
                    yield (row_number, headers, row)
                elif keyed:
                    yield dict(zip(headers, row))
                else:
                    yield row

            # Check integrity
            if integrity:
                violations = []
                size = integrity.get('size')
                hash = integrity.get('hash')
                if size and size != self.__stream.size:
                    violations.append('size "%s"' % self.__stream.size)
                if hash and hash != self.__stream.hash:
                    violations.append('hash "%s"' % self.__stream.hash)
                if violations:
                    message = 'Calculated %s differ(s) from declared value(s)'
                    raise exceptions.IntegrityError(message % ' and '.join(violations))

    def read(self, keyed=False, extended=False, cast=True, limit=None,
             integrity=False, relations=False, foreign_keys_values=False,
             exc_handler=None):
        """Read the whole table and return as array of rows

        > It has the same API as `table.iter` except for

        # Arguments
            limit (int): limit count of rows to read and return

        # Returns
            list[]: returns rows

        """
        result = []
        rows = self.iter(
            keyed=keyed, extended=extended, cast=cast, integrity=integrity,
            relations=relations, foreign_keys_values=foreign_keys_values,
            exc_handler=exc_handler)
        for count, row in enumerate(rows, start=1):
            result.append(row)
            if count == limit:
                break
        return result

    def infer(self, limit=100, confidence=0.75,
              missing_values=config.DEFAULT_MISSING_VALUES,
              guesser_cls=None, resolver_cls=None):
        """Infer a schema for the table.

        It will infer and set Table Schema to `table.schema` based on table data.

        # Arguments
            limit (int): limit rows sample size
            confidence (float): how many casting errors are allowed (as a ratio, between 0 and 1)
            missing_values (str[]): list of missing values (by default `['']`)
            guesser_cls (class): you can implement inferring strategies by
                 providing type-guessing and type-resolving classes [experimental]
            resolver_cls (class): you can implement inferring strategies by
                 providing type-guessing and type-resolving classes [experimental]

        # Returns
            dict: Table Schema descriptor

        """
        if self.__schema is None or self.__headers is None:

            # Infer (tabulator)
            if not self.__storage:
                with self.__stream as stream:
                    if self.__schema is None:
                        self.__schema = Schema({'missingValues': missing_values})
                        self.__schema.infer(stream.sample[:limit],
                                            headers=stream.headers,
                                            confidence=confidence,
                                            guesser_cls=guesser_cls,
                                            resolver_cls=resolver_cls)
                    if self.__headers is None:
                        self.__headers = stream.headers

            # Infer (storage)
            else:
                descriptor = self.__storage.describe(self.__source)
                if self.__schema is None:
                    self.__schema = Schema(descriptor)
                if self.__headers is None:
                    self.__headers = self.__schema.field_names

        return self.__schema.descriptor

    def save(self, target, storage=None, **options):
        """Save data source to file locally in CSV format with `,` (comma) delimiter

        > To save schema use `table.schema.save()`

        # Arguments
            target (str): saving target (e.g. file path)
            storage (None/str): storage name like `sql` or `bigquery`
            options (dict): `tabulator` or storage options

        # Raises
            TableSchemaException: raises an error if there is saving problem

        # Returns
            True/Storage: returns true or storage instance

        """

        # Save (tabulator)
        if storage is None:
            with Stream(self.iter, headers=self.__schema.headers) as stream:
                stream.save(target, **options)
            return True

        # Save (storage)
        else:
            if not isinstance(storage, Storage):
                storage = Storage.connect(storage, **options)
            storage.create(target, self.__schema.descriptor, force=True)
            storage.write(target, self.iter(cast=False))
            return storage

    def index_foreign_keys_values(self, relations):
        """Creates a three-level dictionary of foreign key references

        We create them optimized to speed up validation process in a form of
        `{resource1: {(fk_field1, fk_field2): {(value1, value2): {one_keyedrow}, ... }}}`.

        For each foreign key of the schema it will iterate through the corresponding
        `relations['resource']` to create an index (i.e. a dict) of existing values
        for the foreign fields and store on keyed row for each value combination.

        The optimization relies on the indexation of possible values for one foreign key
        in a hashmap to later speed up resolution.

        This method is public to allow creating the index once to apply it
        on multiple tables charing the same schema
        (typically [grouped resources in datapackage](https://github.com/frictionlessdata/datapackage-py#group))

        # Notes

        - the second key of the output is a tuple of the foreign fields,
            a proxy identifier of the foreign key
        - the same relation resource can be indexed multiple times
            as a schema can contain more than one Foreign Keys
            pointing to the same resource

        # Arguments
            relations (dict):
                dict of foreign key references in a form of
                `{resource1\\: [{field1\\: value1, field2\\: value2}, ...], ...}`.
                It must contain all resources pointed in the foreign keys schema definition.

        # Returns
            dict:
                returns a three-level dictionary of foreign key references
                optimized to speed up validation process in a form of
                `{resource1\\: {(fk_field1, fk_field2)\\: {(value1, value2)\\: {one_keyedrow}, ... }}})`

        """

        # we dont need to load the complete reference table to test relations
        # we can lower payload AND optimize testing foreign keys
        # by preparing the right index based on the foreign key definition
        # foreign_keys are sets of tuples of all possible values in the foreign table
        # foreign keys =
        # [reference] [foreign_keys tuple] = { (foreign_keys_values, ) : one_keyedrow, ... }
        foreign_keys = defaultdict(dict)
        if self.schema:
            for fk in self.schema.foreign_keys:
                # load relation data
                relation = fk['reference']['resource']

                # create a set of foreign keys
                # to optimize we prepare index of existing values
                # this index should use reference + foreign_keys as key
                # cause many foreign keys may use the same reference
                foreign_keys[relation][tuple(fk['reference']['fields'])] = {}
                for row in relations[relation]:
                    key = tuple([row[foreign_field] for foreign_field in fk['reference']['fields']])
                    # here we should chose to pick the first or nth row which match
                    # previous implementation picked the first, so be it
                    if key not in foreign_keys[relation][tuple(fk['reference']['fields'])]:
                        foreign_keys[relation][tuple(fk['reference']['fields'])][key] = row
        return foreign_keys

    # Private

    def __apply_processors(self, iterator, cast=True, exc_handler=None):

        # Apply processors to iterator
        def builtin_processor(extended_rows):
            for row_number, headers, row in extended_rows:
                if self.__schema and cast:
                    row = self.__schema.cast_row(
                        row, row_number=row_number, exc_handler=exc_handler)
                yield (row_number, headers, row)
        processors = [builtin_processor] + self.__post_cast
        for processor in processors:
            iterator = processor(iterator)

        return iterator


# Internal

def _create_unique_fields_cache(schema):
    primary_key_indexes = []
    cache = {}

    # Unique
    for index, field in enumerate(schema.fields):
        if field.name in schema.primary_key:
            primary_key_indexes.append(index)
        if field.constraints.get('unique'):
            cache[tuple([index])] = {
                'name': field.name,
                'data': set(),
            }

    # Primary key
    if primary_key_indexes:
        cache[tuple(primary_key_indexes)] = {
            'name': ', '.join(schema.primary_key),
            'data': set(),
        }

    return cache


def _resolve_relations(row, headers, foreign_keys_values, foreign_key):

    # Prepare helpers - needed data structures
    keyed_row = OrderedDict(zip(headers, row))
    # local values of the FK
    local_values = tuple(keyed_row[f] for f in foreign_key['fields'])
    if set(local_values) != {None}:
        # test existence into the foreign
        relation = foreign_key['reference']['resource']
        keys = tuple(foreign_key['reference']['fields'])
        foreign_values = foreign_keys_values[relation][keys]
        if local_values in foreign_values:
            return foreign_values[local_values]
        else:
            return None
    else:
        # empty values for all keys, return original values
        return row
