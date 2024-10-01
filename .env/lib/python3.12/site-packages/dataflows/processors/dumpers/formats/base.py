import logging
import json
import datetime
from functools import partial


def identity(x):
    return x


def json_dumps(x):
    return json.dumps(x, ensure_ascii=False)


def comma_separated(x):
    if x is None:
        return None
    elif isinstance(x, list):
        if all(isinstance(i, (str, int, float)) for i in x):
            return ', '.join(str(i) for i in x)
    return json_dumps(x)


class FileFormat():

    PYTHON_DIALECT = {}
    NULL_VALUE = None
    SERIALIZERS = {}
    FILE_MODE = 'w+'

    def __init__(self, writer, schema, temporal_format_property=None, default_serializer=str, **kwargs):

        # Set properties
        self.writer = writer
        self.headers = [f.name for f in schema.fields]
        self.fields = dict((f.name, f) for f in schema.fields)
        self.temporal_format_property = temporal_format_property
        self.missing_values = schema.descriptor.get('missingValues', [])

        # Set fields' serializers
        for field in schema.fields:
            serializer = self.SERIALIZERS.get(field.type, default_serializer)
            if self.temporal_format_property:
                if field.type in ['datetime', 'date', 'time']:
                    format = field.descriptor.get(self.temporal_format_property, None)
                    if format:
                        strftime = getattr(datetime, field.type).strftime
                        serializer = partial(strftime, format=format)
            field.descriptor['serializer'] = serializer

    @classmethod
    def prepare_resource(cls, resource):
        for field in resource.descriptor['schema']['fields']:
            field.update(cls.PYTHON_DIALECT.get(field['type'], {}))

    def __transform_row(self, row):
        try:
            return dict((k, self.__transform_value(v, self.fields[k]))
                        for k, v in row.items())
        except Exception:
            logging.exception('Failed to transform row %r', row)
            raise

    def __transform_value(self, value, field):
        if value is None:
            return self.NULL_VALUE
        # It supports a `tableschema`'s mode of perserving missing values
        # https://github.com/frictionlessdata/tableschema-py#experimental
        if value in self.missing_values:
            return value
        return field.descriptor['serializer'](value)

    def write_transformed_row(self, *_):
        raise NotImplementedError()

    def write_row(self, row):
        transformed_row = self.__transform_row(row)
        self.write_transformed_row(transformed_row)
