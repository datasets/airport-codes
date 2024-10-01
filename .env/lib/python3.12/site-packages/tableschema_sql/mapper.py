# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json

import six
import tableschema
import sqlalchemy as sa
from sqlalchemy import CheckConstraint as Check
from sqlalchemy.dialects.postgresql import ARRAY, JSON, JSONB, UUID


# Module API

class Mapper(object):

    # Public

    def __init__(self, prefix, dialect='sqlite'):
        """Mapper to convert/restore FD entities to/from SQL entities
        """
        self.__prefix = prefix
        self.__dialect = dialect

    def convert_bucket(self, bucket):
        """Convert bucket to SQL
        """
        return self.__prefix + bucket

    def convert_descriptor(self, bucket, descriptor, index_fields=[], autoincrement=None):
        """Convert descriptor to SQL
        """

        # Prepare
        columns = []
        indexes = []
        fallbacks = []
        constraints = []
        column_mapping = {}
        table_name = self.convert_bucket(bucket)
        comment = _get_comment(descriptor.get('title', ''), descriptor.get('description', ''))
        schema = tableschema.Schema(descriptor)

        # Autoincrement
        if autoincrement is not None:
            columns.append(sa.Column(
                autoincrement, sa.Integer, autoincrement=True, nullable=False))

        # Fields
        for field in schema.fields:
            column_type = self.convert_type(field.type)
            if not column_type:
                column_type = sa.Text
                fallbacks.append(field.name)
            nullable = not field.required
            comment = _get_field_comment(field)
            unique = field.constraints.get('unique', False)
            checks = []
            for name, value in field.constraints.items():
                if name == 'minLength':
                    checks.append(Check('LENGTH("%s") >= %s' % (field.name, value)))
                elif name == 'maxLength':
                    checks.append(Check('LENGTH("%s") <= %s' % (field.name, value)))
                elif name == 'minimum':
                    checks.append(Check('"%s" >= %s' % (field.name, value)))
                elif name == 'maximum':
                    checks.append(Check('"%s" <= %s' % (field.name, value)))
                elif name == 'pattern':
                    if self.__dialect in ['postgresql']:
                        checks.append(Check('"%s" ~ \'%s\'' % (field.name, value)))
                    else:
                        checks.append(Check('"%s" REGEXP \'%s\'' % (field.name, value)))
                elif name == 'enum':
                    if self.__dialect in ['sqlite']:
                        checks.append(Check(sa.text('"%s" in (:values)' % field.name)
                                              .bindparams(sa.bindparam(key="values", value=value, expanding=True))))
                    else:
                        column_type = sa.Enum(*value, name='%s_%s_enum' % (table_name, field.name))
            column = sa.Column(*([field.name, column_type] + checks),
                nullable=nullable, comment=comment, unique=unique)
            columns.append(column)
            column_mapping[field.name] = column

        # Primary key
        pk = descriptor.get('primaryKey', None)
        if pk is not None:
            if isinstance(pk, six.string_types):
                pk = [pk]
        if autoincrement is not None:
            if pk is not None:
                pk = [autoincrement] + pk
            else:
                pk = [autoincrement]
        if pk is not None:
            constraint = sa.PrimaryKeyConstraint(*pk)
            constraints.append(constraint)

        # Foreign keys
        if self.__dialect in ['postgresql', 'sqlite']:
            fks = descriptor.get('foreignKeys', [])
            for fk in fks:
                fields = fk['fields']
                resource = fk['reference']['resource']
                foreign_fields = fk['reference']['fields']
                if isinstance(fields, six.string_types):
                    fields = [fields]
                if resource != '':
                    table_name = self.convert_bucket(resource)
                if isinstance(foreign_fields, six.string_types):
                    foreign_fields = [foreign_fields]
                composer = lambda field: '.'.join([table_name, field])
                foreign_fields = list(map(composer, foreign_fields))
                constraint = sa.ForeignKeyConstraint(fields, foreign_fields)
                constraints.append(constraint)

        # Indexes
        if self.__dialect in ('postgresql', 'sqlite'):
            for index, index_definition in enumerate(index_fields):
                name = table_name + '_ix%03d' % index
                index_columns = [column_mapping[field] for field in index_definition]
                indexes.append(sa.Index(name, *index_columns))

        return columns, constraints, indexes, fallbacks, comment

    def convert_row(self, keyed_row, schema, fallbacks):
        """Convert row to SQL
        """
        for key, value in list(keyed_row.items()):
            field = schema.get_field(key)
            if not field:
                del keyed_row[key]
            if key in fallbacks:
                value = _uncast_value(value, field=field)
            else:
                value = field.cast_value(value)
            keyed_row[key] = value
        return keyed_row

    def convert_type(self, type):
        """Convert type to SQL
        """

        # Default dialect
        mapping = {
            'any': sa.Text,
            'array': None,
            'boolean': sa.Boolean,
            'date': sa.Date,
            'datetime': sa.DateTime,
            'duration': None,
            'geojson': None,
            'geopoint': None,
            'integer': sa.Integer,
            'number': sa.Float,
            'object': None,
            'string': sa.Text,
            'time': sa.Time,
            'year': sa.Integer,
            'yearmonth': None,
        }

        # Postgresql dialect
        if self.__dialect == 'postgresql':
            mapping.update({
                'array': JSONB,
                'geojson': JSONB,
                'number': sa.Numeric,
                'object': JSONB,
            })

        # Not supported type
        if type not in mapping:
            message = 'Field type "%s" is not supported'
            raise tableschema.exceptions.StorageError(message % type)

        return mapping[type]

    def restore_bucket(self, table_name):
        """Restore bucket from SQL
        """
        if table_name.startswith(self.__prefix):
            return table_name.replace(self.__prefix, '', 1)
        return None

    def restore_descriptor(self, table_name, columns, constraints, autoincrement=None):
        """Restore descriptor from SQL
        """

        # Fields
        fields = []
        for column in columns:
            if column.name == autoincrement:
                continue
            field_type = self.restore_type(column.type)
            field = {'name': column.name, 'type': field_type}
            if not column.nullable:
                field['constraints'] = {'required': True}
            fields.append(field)

        # Primary key
        pk = []
        for constraint in constraints:
            if isinstance(constraint, sa.PrimaryKeyConstraint):
                for column in constraint.columns:
                    if column.name == autoincrement:
                        continue
                    pk.append(column.name)

        # Foreign keys
        fks = []
        if self.__dialect in ['postgresql', 'sqlite']:
            for constraint in constraints:
                if isinstance(constraint, sa.ForeignKeyConstraint):
                    resource = ''
                    own_fields = []
                    foreign_fields = []
                    for element in constraint.elements:
                        own_fields.append(element.parent.name)
                        if element.column.table.name != table_name:
                            resource = self.restore_bucket(element.column.table.name)
                        foreign_fields.append(element.column.name)
                    if len(own_fields) == len(foreign_fields) == 1:
                        own_fields = own_fields.pop()
                        foreign_fields = foreign_fields.pop()
                    fks.append({
                        'fields': own_fields,
                        'reference': {'resource': resource, 'fields': foreign_fields},
                    })

        # Desscriptor
        descriptor = {}
        descriptor['fields'] = fields
        if len(pk) > 0:
            if len(pk) == 1:
                pk = pk.pop()
            descriptor['primaryKey'] = pk
        if len(fks) > 0:
            descriptor['foreignKeys'] = fks

        return descriptor

    def restore_row(self, row, schema, autoincrement):
        """Restore row from SQL
        """
        row = list(row)
        for index, field in enumerate(schema.fields, start=1 if autoincrement else 0):
            if self.__dialect == 'postgresql':
                if field.type in ['array', 'object']:
                    continue
            row[index] = field.cast_value(row[index])
        return row

    def restore_type(self, type):
        """Restore type from SQL
        """

        # All dialects
        mapping = {
            ARRAY: 'array',
            sa.Boolean: 'boolean',
            sa.Date: 'date',
            sa.DateTime: 'datetime',
            sa.Float: 'number',
            sa.Integer: 'integer',
            JSONB: 'object',
            JSON: 'object',
            sa.Numeric: 'number',
            sa.Text: 'string',
            sa.Time: 'time',
            sa.VARCHAR: 'string',
            UUID: 'string',
        }

        # Get field type
        field_type = None
        for key, value in mapping.items():
            if isinstance(type, key):
                field_type = value

        # Not supported
        if field_type is None:
            message = 'Type "%s" is not supported'
            raise tableschema.exceptions.StorageError(message % type)

        return field_type


# Internal

def _uncast_value(value, field):
    # Eventially should be moved to:
    # https://github.com/frictionlessdata/tableschema-py/issues/161
    if isinstance(value, (list, dict)):
        value = json.dumps(value)
    else:
        value = str(value)
    return value


def _get_field_comment(field, separator=' - '):
    """
    Create SQL comment from field's title and description

    :param field: tableschema-py Field, with optional 'title' and 'description' values
    :param separator:
    :return:

    >>> _get_field_comment(tableschema.Field({'title': 'my_title', 'description': 'my_desc'}))
    'my_title - my_desc'
    >>> _get_field_comment(tableschema.Field({'title': 'my_title', 'description': None}))
    'my_title'
    >>> _get_field_comment(tableschema.Field({'title': '', 'description': 'my_description'}))
    'my_description'
    >>> _get_field_comment(tableschema.Field({}))
    ''
    """
    title = field.descriptor.get('title') or ''
    description = field.descriptor.get('description') or ''
    return _get_comment(description, title, separator)


def _get_comment(description, title, separator=' - '):
    if title == '':
        return description
    if description == '':
        return title
    return title + separator + description
