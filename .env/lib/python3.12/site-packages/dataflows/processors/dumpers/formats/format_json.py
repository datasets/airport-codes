import json
import isodate
from pathlib import Path
import datetime
import decimal

from dataflows.helpers.extended_json import (
    DATETIME_F_FORMAT, DATE_F_FORMAT, TIME_F_FORMAT,
    DATETIME_P_FORMAT, DATE_P_FORMAT, TIME_P_FORMAT,
)

from .base import FileFormat, identity


class JSONFormat(FileFormat):

    SERIALIZERS = {
        'datetime': lambda d: d.strftime(DATETIME_F_FORMAT),
        'date': lambda d: d.strftime(DATE_F_FORMAT),
        'time': lambda d: d.strftime(TIME_F_FORMAT),
        'number': float,
        'duration': lambda d: isodate.duration_isoformat(d),
        'geopoint': lambda d: list(map(float, d)),
        'yearmonth': lambda d: '{:04d}-{:02d}'.format(*d),
    }

    NULL_VALUE = None

    PYTHON_DIALECT = {
        'date': {
            'format': DATE_P_FORMAT
        },
        'time': {
            'format': TIME_P_FORMAT
        },
        'datetime': {
            'format': DATETIME_P_FORMAT
        },
    }

    def __init__(self, file, schema, **options):
        self.initialize_file(file)
        super(JSONFormat, self).__init__(file, schema, default_serializer=identity, **options)

    def initialize_file(self, file):
        file.write('[')
        file.__first = True

    @classmethod
    def prepare_resource(cls, resource):
        descriptor = resource.descriptor
        descriptor['encoding'] = 'utf-8'
        descriptor['path'] = str(Path(descriptor['path']).with_suffix('.json'))
        descriptor['format'] = 'json'
        descriptor['mediatype'] = 'text/json'
        descriptor['profile'] = 'tabular-data-resource'
        super(JSONFormat, cls).prepare_resource(resource)

    def write_transformed_row(self, transformed_row):
        if not self.writer.__first:
            self.writer.write(',')
        else:
            self.writer.__first = False
        self.writer.write(json.dumps(transformed_row, sort_keys=True, ensure_ascii=True, cls=self.Encoder))

    def finalize_file(self):
        self.writer.write(']')

    # create encoder class
    class Encoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, (datetime.date, datetime.time, datetime.datetime)):
                return o.isoformat()
            elif isinstance(o, isodate.Duration):
                return isodate.duration_isoformat(o)
            elif isinstance(o, decimal.Decimal):
                return float(o)
            return super().default(o)
