from inspect import signature

import re

from ..helpers.resource_matcher import ResourceMatcher
from .. import DataStreamProcessor, schema_validator


class set_type(DataStreamProcessor):

    def __init__(self, name, resources=-1, regex=True, on_error=None, transform=None, **options):
        super(set_type, self).__init__()
        if not regex:
            name = re.escape(name)
        self.name = re.compile(f'^{name}$')
        self.options = options
        self.resources = resources
        self.field_names = dict()
        self.on_error = on_error
        self.transform = self.wrap_transformer(transform) if transform else None

    def wrap_transformer(self, transform):
        assert callable(transform)
        try:
            sig = signature(transform).parameters
        except Exception:
            sig = set()

        def func(v, field_name=None, row=None):
            kw = {}
            if 'row' in sig:
                kw['row'] = row
            if 'field_name' in sig:
                kw['field_name'] = field_name
            return transform(v, **kw)
        return func

    def transformer(self, rows, field_names):
        for row in rows:
            for field_name in field_names:
                row[field_name] = self.transform(row.get(field_name), field_name=field_name, row=row)
            yield row

    def process_resources(self, resources):
        for res in resources:
            if self.matcher.match(res.res.name):
                field_names = self.field_names.get(res.res.name, [])
                if len(field_names) > 0:
                    it = res
                    if self.transform is not None:
                        it = self.transformer(it, field_names)
                    yield schema_validator(res.res, it,
                                           field_names=field_names,
                                           on_error=self.on_error)
                else:
                    yield res
            else:
                yield res

    def process_datapackage(self, dp):
        dp = super(set_type, self).process_datapackage(dp)
        self.matcher = ResourceMatcher(self.resources, dp)
        added = False
        for res in dp.descriptor['resources']:
            if self.matcher.match(res['name']):
                for field in res['schema']['fields']:
                    if self.name.match(field['name']):
                        field.update(self.options)
                        self.field_names.setdefault(res['name'], []).append(field['name'])
                        added = True
        assert added, 'Failed to find field {} in schema'.format(self.name)
        return dp
