import csv
import json
import isodate
from pathlib import Path

from dataflows.helpers.extended_json import (
    DATETIME_F_FORMAT, DATE_F_FORMAT, TIME_F_FORMAT,
    DATETIME_P_FORMAT, DATE_P_FORMAT, TIME_P_FORMAT,
)

from .base import FileFormat, json_dumps


class CsvTitlesDictWriter(csv.DictWriter):
    def __init__(self, *args, **kwargs):
        self.fieldtitles = kwargs.pop('fieldtitles')
        super().__init__(*args, **kwargs)

    def writeheader(self):
        header = dict(zip(self.fieldnames, self.fieldtitles))
        self.writerow(header)


class CSVFormat(FileFormat):

    SERIALIZERS = {
        'array': json_dumps,
        'object': json_dumps,
        'datetime': lambda d: d.strftime(DATETIME_F_FORMAT),
        'date': lambda d: d.strftime(DATE_F_FORMAT),
        'time': lambda d: d.strftime(TIME_F_FORMAT),
        'duration': lambda d: isodate.duration_isoformat(d),
        'geopoint': lambda d: '{}, {}'.format(*d),
        'geojson': json.dumps,
        'year': lambda d: '{:04d}'.format(d),
        'yearmonth': lambda d: '{:04d}-{:02d}'.format(*d),
    }
    NULL_VALUE = ''

    PYTHON_DIALECT = {
        'number': {
            'decimalChar': '.',
            'groupChar': ''
        },
        'date': {
            'format': DATE_P_FORMAT
        },
        'time': {
            'format': TIME_P_FORMAT
        },
        'datetime': {
            'format': DATETIME_P_FORMAT
        },
        'boolean': {
            'trueValues': ['True'],
            'falseValues': ['False']
        }
    }

    def __init__(self, file, schema, use_titles=False, **options):
        headers = [f.name for f in schema.fields]
        if use_titles:
            titles = [f.descriptor.get('title', f.name) for f in schema.fields]
            csv_writer = CsvTitlesDictWriter(file, headers, fieldtitles=titles)
        else:
            csv_writer = csv.DictWriter(file, headers)
        csv_writer.writeheader()
        super(CSVFormat, self).__init__(csv_writer, schema, **options)

    @classmethod
    def prepare_resource(cls, resource):
        descriptor = resource.descriptor
        descriptor['encoding'] = 'utf-8'
        descriptor['path'] = str(Path(descriptor['path']).with_suffix('.csv'))
        descriptor['format'] = 'csv'
        descriptor['dialect'] = dict(
            lineTerminator='\r\n',
            delimiter=',',
            doubleQuote=True,
            quoteChar='"',
            skipInitialSpace=False
        )
        super(CSVFormat, cls).prepare_resource(resource)

    def write_transformed_row(self, transformed_row):
        self.writer.writerow(transformed_row)

    def finalize_file(self):
        pass
