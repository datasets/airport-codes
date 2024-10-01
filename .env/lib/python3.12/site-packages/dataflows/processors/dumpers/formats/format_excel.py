import json
from datapackage.resource import Resource
import isodate
import openpyxl
from pathlib import Path

from dataflows.helpers.extended_json import (
    DATETIME_P_FORMAT, DATE_P_FORMAT, TIME_P_FORMAT,
)

from .base import FileFormat, identity, comma_separated, json_dumps


class ExcelFormat(FileFormat):

    SERIALIZERS = {
        'array': comma_separated,
        'object': json_dumps,
        'datetime': identity,
        'date': identity,
        'time': identity,
        'duration': lambda d: isodate.duration_isoformat(d),
        'geopoint': lambda d: '{}, {}'.format(*d),
        'geojson': json.dumps,
        'year': identity,
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
    FILE_MODE = 'wb+'

    def __init__(self, file, schema, use_titles=False, **options):
        self.resource: Resource = options['resource']
        self.tmpfile = file
        self.sheetname = options.get('sheetname') or self.resource.name or 'Sheet1'
        self.update_existing = options.get('update_existing')
        try:
            if self.update_existing and Path(self.update_existing).exists():
                self.workbook = openpyxl.load_workbook(self.update_existing)
            else:
                self.workbook = openpyxl.Workbook()
                for sheet in self.workbook.worksheets:
                    self.workbook.remove(sheet)
            if self.sheetname in self.workbook.sheetnames:
                self.workbook.remove(self.workbook[self.sheetname])
            worksheet = self.workbook.create_sheet(self.sheetname)
            self.workbook.save(filename=self.tmpfile.name)
        finally:
            self.workbook.close()

        super(ExcelFormat, self).__init__(worksheet, schema, **options)

        if use_titles:
            headers = dict((f.name, f.descriptor.get('title', f.name)) for f in schema.fields)
        else:
            headers = dict((f.name, f.name) for f in schema.fields)
        self.write_transformed_row(headers)

    @classmethod
    def prepare_resource(cls, resource):
        descriptor = resource.descriptor
        descriptor['path'] = str(Path(descriptor['path']).with_suffix('.xlsx'))
        descriptor['format'] = 'xlsx'
        super(ExcelFormat, cls).prepare_resource(resource)

    def write_transformed_row(self, transformed_row):
        self.writer.append(transformed_row[k] for k in self.fields)

    def finalize_file(self):
        self.workbook.save(self.tmpfile.name)
        self.workbook.close()
